#!/usr/bin/env python3
"""
Code command handler for RaySSH CLI.
Provides code-server functionality.
"""

import os
import sys
import time
import random
import subprocess
import urllib.parse
from typing import List, Optional

import ray
from ray.job_submission import JobSubmissionClient

from agent.code_server import CodeServerActor
from utils import (
    ensure_ray_initialized,
    fetch_cluster_nodes,
    get_head_node_id,
    parse_n_gpus_from_env,
    load_last_session_preferred_ip,
    find_node_by_ip,
    write_last_session_node_ip,
    get_ray_dashboard_info,
    apply_require_constraints_to_actor_options,
)


def _select_worker_node_id(allow_head_if_no_worker: bool) -> str:
    # Ensure Ray is initialized prior to state API
    import os as _os
    import ray as _ray

    if not _ray.is_initialized():
        addr = _os.environ.get("RAY_ADDRESS")
        ensure_ray_initialized(ray_address=addr, working_dir=None)
    nodes, head_node_id = fetch_cluster_nodes()
    if not nodes:
        raise RuntimeError("No alive Ray nodes found in the cluster")

    # Build worker-only list
    worker_node_ids = []
    for n in nodes:
        nid = n.get("NodeID")
        if not nid:
            continue
        if head_node_id and nid == head_node_id:
            continue
        worker_node_ids.append(nid)

    if worker_node_ids:
        return random.choice(worker_node_ids)

    if allow_head_if_no_worker:
        head = head_node_id or (nodes[0].get("NodeID") if nodes else None)
        if not head:
            raise RuntimeError("Could not determine head node")
        return head

    raise RuntimeError(
        "No worker nodes available. Use '-0' before the command to allow placing on the head node."
    )


def _submit_codeserver_install_job(
    ray_address: str, worker_node_id: str, archive_path: str
) -> bool:
    """Submit a Ray job that uploads the code-server archive and installs it under ~/.rayssh on the target node.
    Returns True on success, False otherwise.
    """
    try:
        # Create a temporary workdir containing the archive and the installer script
        import tempfile, shutil, textwrap

        ship_dir = tempfile.mkdtemp(prefix="rayssh_cs_job_")
        archive_name = os.path.basename(archive_path)
        staged_archive = os.path.join(ship_dir, archive_name)
        shutil.copy2(archive_path, staged_archive)

        installer_py = os.path.join(ship_dir, "install_codeserver_job.py")
        with open(installer_py, "w") as f:
            f.write(
                textwrap.dedent(f"""
            import os, tarfile, shutil
            import ray

            @ray.remote
            class Installer:
                def run(self):
                    try:
                        cwd = os.getcwd()
                        # Find shipped archive in cwd
                        archive = None
                        for fn in os.listdir(cwd):
                            if fn.startswith("code-server-") and fn.endswith(".tar.gz"):
                                p = os.path.join(cwd, fn)
                                if os.path.isfile(p):
                                    archive = p
                                    break
                        if not archive:
                            return {{"success": False, "error": "archive not found in job working_dir"}}
                        install_prefix = os.path.expanduser("~/.rayssh")
                        bin_dir = os.path.join(install_prefix, "bin")
                        lib_dir = os.path.join(install_prefix, "lib")
                        os.makedirs(bin_dir, exist_ok=True)
                        os.makedirs(lib_dir, exist_ok=True)
                        # Extract
                        with tarfile.open(archive, "r:gz") as tar:
                            tar.extractall(lib_dir)
                        # Find extracted folder and normalize
                        extracted = None
                        for d in os.listdir(lib_dir):
                            if d.startswith("code-server-") and os.path.isdir(os.path.join(lib_dir, d)):
                                extracted = os.path.join(lib_dir, d)
                                break
                        if not extracted:
                            return {{"success": False, "error": "extracted dir not found"}}
                        # Symlink binary
                        src = os.path.join(extracted, "bin", "code-server")
                        dst = os.path.join(bin_dir, "code-server")
                        if os.path.exists(dst) or os.path.islink(dst):
                            try:
                                os.remove(dst)
                            except Exception:
                                pass
                        os.symlink(src, dst)
                        try:
                            os.chmod(src, 0o755)
                        except Exception:
                            pass
                        return {{"success": True, "binary": dst}}
                    except Exception as e:
                        return {{"success": False, "error": str(e)}}

            def main():
                # Connect to existing Ray cluster inside the job
                ray.init(address="auto")
                # Place installer on the desired node via NodeAffinitySchedulingStrategy
                actor = Installer.options(
                    scheduling_strategy=ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
                        node_id="{worker_node_id}", soft=False
                    )
                ).remote()
                return ray.get(actor.run.remote())

            if __name__ == "__main__":
                import json
                res = main()
                print(json.dumps(res))
            """)
            )

        # Build runtime_env for the job (upload ship_dir)
        job_runtime_env = {"working_dir": ship_dir}

        # Submit job via CLI so we do not disturb current Ray Client session context
        cmd = [
            "ray",
            "job",
            "submit",
            "--address",
            ray_address,
            "--working-dir",
            ship_dir,
            "--entrypoint-num-cpus=0.5",
            "--",
            "python",
            "install_codeserver_job.py",
        ]
        print("🚚 Uploading archive and running installer job...")
        print(f"📦 Staged: {staged_archive}")
        # Run the job (the --working-dir handles upload); no special env needed
        proc = subprocess.run(cmd, cwd=ship_dir)
        if proc.returncode != 0:
            print("❌ Installer job failed")
            return False
        print("✅ Installer job finished")
        return True
    except Exception as e:
        print(f"❌ Failed to submit installer job: {e}")
        return False


def _install_codeserver_via_ray_client(
    ray_address: str, worker_node_id: str, archive_path: str
) -> bool:
    """Install code-server using Ray Client (runtime_env.working_dir upload) from a separate Python process.
    Returns True on success.
    """
    try:
        import tempfile, shutil, textwrap, json as _json

        ship_dir = tempfile.mkdtemp(prefix="rayssh_cs_client_")
        archive_name = os.path.basename(archive_path)
        staged_archive = os.path.join(ship_dir, archive_name)
        shutil.copy2(archive_path, staged_archive)

        script_path = os.path.join(ship_dir, "install_codeserver_client.py")
        script_base = textwrap.dedent("""
        import os, tarfile, json
        import logging
        # Suppress Ray/absl/glog noise as much as possible
        os.environ["RAY_DISABLE_IMPORT_WARNING"] = "1"
        os.environ["RAY_DEDUP_LOGS"] = "0"
        os.environ["GLOG_minloglevel"] = "3"
        os.environ["GLOG_logtostderr"] = "0"
        os.environ["RAY_RAYLET_LOG_LEVEL"] = "FATAL"
        os.environ["RAY_CORE_WORKER_LOG_LEVEL"] = "FATAL"
        os.environ["GRPC_VERBOSITY"] = "ERROR"
        logging.getLogger("ray").setLevel(logging.ERROR)

        from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
        import ray

        RAY_ADDRESS = __RAY_ADDRESS__
        WORKING_DIR = __WORKING_DIR__
        TARGET_NODE_ID = __TARGET_NODE_ID__

        @ray.remote
        class Installer:
            def run(self):
                try:
                    cwd = os.getcwd()
                    # Find shipped archive in cwd
                    archive = None
                    for fn in os.listdir(cwd):
                        if fn.startswith("code-server-") and fn.endswith(".tar.gz"):
                            p = os.path.join(cwd, fn)
                            if os.path.isfile(p):
                                archive = p
                                break
                    if not archive:
                        return {"success": False, "error": "archive not found in working_dir"}
                    install_prefix = os.path.expanduser("~/.rayssh")
                    bin_dir = os.path.join(install_prefix, "bin")
                    lib_dir = os.path.join(install_prefix, "lib")
                    os.makedirs(bin_dir, exist_ok=True)
                    os.makedirs(lib_dir, exist_ok=True)
                    # Extract
                    with tarfile.open(archive, "r:gz") as tar:
                        tar.extractall(lib_dir)
                    # Normalize
                    extracted = None
                    for d in os.listdir(lib_dir):
                        if d.startswith("code-server-") and os.path.isdir(os.path.join(lib_dir, d)):
                            extracted = os.path.join(lib_dir, d)
                            break
                    if not extracted:
                        return {"success": False, "error": "extracted dir not found"}
                    # Symlink binary
                    src = os.path.join(extracted, "bin", "code-server")
                    dst = os.path.join(bin_dir, "code-server")
                    if os.path.exists(dst) or os.path.islink(dst):
                        try:
                            os.remove(dst)
                        except Exception:
                            pass
                    os.symlink(src, dst)
                    try:
                        os.chmod(src, 0o755)
                    except Exception:
                        pass
                    return {"success": True, "binary": dst}
                except Exception as e:
                    return {"success": False, "error": str(e)}

        def main():
            # Connect with working_dir so files are uploaded by Ray Client
            ray.init(address=RAY_ADDRESS, runtime_env={"working_dir": WORKING_DIR}, logging_level="FATAL", log_to_driver=False)
            actor = Installer.options(
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    node_id=TARGET_NODE_ID, soft=False
                )
            ).remote()
            res = ray.get(actor.run.remote())
            print(json.dumps(res))
            return 0 if res.get("success") else 1

        if __name__ == "__main__":
            raise SystemExit(main())
        """)
        script = (
            script_base.replace("__RAY_ADDRESS__", repr(ray_address))
            .replace("__WORKING_DIR__", repr(ship_dir))
            .replace("__TARGET_NODE_ID__", repr(worker_node_id))
        )
        with open(script_path, "w") as f:
            f.write(script)

        print("🚚 Uploading archive via Ray Client and installing on target node...")
        # Capture installer output to suppress Ray's noisy logs
        proc = subprocess.run(
            [sys.executable, script_path],
            cwd=ship_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        if proc.returncode != 0:
            # Print a concise error summary (keep logs hidden unless needed)
            err_tail = (proc.stderr or "").splitlines()[-5:]
            if err_tail:
                print("❌ Ray Client installer failed:")
                for ln in err_tail:
                    print(f"   {ln}")
            else:
                print("❌ Ray Client installer failed")
            return False
        print("✅ Ray Client installer finished")
        return True
    except Exception as e:
        print(f"❌ Failed Ray Client installer: {e}")
        return False


def _apply_job_runtime_env_to_actor_options(
    actor_options: dict, job_or_submission_id: Optional[str]
) -> dict:
    """If a job/submission id is provided, fetch its runtime_env via JobSubmissionClient
    and apply working_dir/uris/env_vars to the given actor options.
    """
    if not job_or_submission_id:
        return {"success": True}
    try:
        dash = get_ray_dashboard_info() or {}
        base_url = dash.get("dashboard_url")
        if not base_url and dash.get("host") and dash.get("port"):
            base_url = f"http://{dash['host']}:{dash['port']}"
        if not base_url:
            raise RuntimeError(
                "Ray dashboard address not found for JobSubmissionClient"
            )
        print("💼 job_or_submission_id:", job_or_submission_id)
        # Ensure RAY_ADDRESS does not override our explicit dashboard base_url
        original_ray_address = os.environ.get("RAY_ADDRESS")
        temporarily_cleared = False
        try:
            if (
                original_ray_address
                and isinstance(base_url, str)
                and base_url.startswith("http")
            ):
                temporarily_cleared = True
                del os.environ["RAY_ADDRESS"]
            client = JobSubmissionClient(base_url)
            details = client.get_job_info(job_or_submission_id)
        finally:
            if temporarily_cleared and original_ray_address is not None:
                os.environ["RAY_ADDRESS"] = original_ray_address
        if not details:
            return {"success": False, "error": "Job/submission not found"}
        actor_rt = getattr(details, "runtime_env", None) or {}
        if actor_rt:
            actor_options["runtime_env"] = actor_rt
            # Keep user-facing log concise to key bits
            print(
                "🧩 Inheriting job runtime_env (working_dir/env_vars) for code-server"
            )
        return {"success": True, "runtime_env": actor_rt}
    except Exception as e:
        print(f"⚠️  Failed to inherit job runtime_env: {e}", file=sys.stderr)
        return {"success": False, "error": str(e)}


def handle_code_command(argv: List[str]) -> int:
    allow_head_if_no_worker = False
    args = argv
    if len(argv) >= 2 and argv[0] == "-0" and argv[1] == "code":
        allow_head_if_no_worker = True
        args = argv[2:]
    elif len(argv) >= 1 and argv[0] == "code":
        args = argv[1:]

    quick = False
    explicit_ip: Optional[str] = None
    code_path: Optional[str] = None
    job_or_submission_id: Optional[str] = None
    # Extract non-flag tokens (order-preserving); prefer first as IP if it looks like one
    non_flags: list[str] = []
    for a in args:
        if a == "-q":
            quick = True
        else:
            non_flags.append(a)
    if non_flags:
        # If the first non-flag looks like an IP, treat it as target IP
        from utils import is_valid_ip as _is_ip

        if _is_ip(non_flags[0]):
            explicit_ip = non_flags[0]
            if len(non_flags) >= 2:
                candidate = non_flags[1]
                if os.path.exists(candidate):
                    code_path = candidate
                else:
                    job_or_submission_id = candidate
        else:
            # No explicit IP; interpret first token as either a local path or a job/submission id
            candidate = non_flags[0]
            if os.path.exists(candidate):
                code_path = candidate
            else:
                job_or_submission_id = candidate

    # Initialize Ray first
    ray_address_env = os.environ.get("RAY_ADDRESS")
    try:
        if ray_address_env:
            # Only upload working_dir if user explicitly specified a path
            if code_path:
                ensure_ray_initialized(
                    ray_address=ray_address_env, working_dir=code_path
                )
            else:
                ensure_ray_initialized(ray_address=ray_address_env, working_dir=None)
        else:
            if code_path:
                print(
                    "Warning: [path] is only used in Ray Client mode (RAY_ADDRESS). Ignoring path."
                )
            ensure_ray_initialized(ray_address=ray_address_env)

    except Exception as e:
        msg = str(e)
        if "Version mismatch" in msg and "Python" in msg:
            print(
                "❌ Ray/Python version mismatch between this process and the running cluster.",
                file=sys.stderr,
            )
        else:
            print(f"Error initializing Ray: {e}", file=sys.stderr)
        return 1

    # Now select the target node (after Ray is initialized)
    try:
        worker_node_id = None
        # Highest priority: explicit IP from CLI
        if explicit_ip:
            node = find_node_by_ip(explicit_ip)
            if not node or not node.get("Alive"):
                print(
                    f"❌ Specified IP not found/alive: {explicit_ip}", file=sys.stderr
                )
                return 1
            worker_node_id = node.get("NodeID")
        else:
            # Next: last session preferred IP
            prefer_ip = load_last_session_preferred_ip()
            if prefer_ip:
                node = find_node_by_ip(prefer_ip)
                if node and node.get("Alive"):
                    worker_node_id = node.get("NodeID")
            # Fallback: pick a worker (or head with -0)
            if not worker_node_id:
                worker_node_id = _select_worker_node_id(allow_head_if_no_worker)
    except Exception as e:
        print(f"Error selecting node: {e}", file=sys.stderr)
        return 1

    # Parse GPU requirements from environment
    n_gpus = parse_n_gpus_from_env()
    if n_gpus is not None:
        print(f"🎛️ GPUs requested: {n_gpus}")

    # Singleton CodeServerActor per node, include node IP in name
    try:
        nodes, _ = fetch_cluster_nodes()
        node_ip = None
        for n in nodes:
            if n.get("NodeID") == worker_node_id:
                node_ip = n.get("NodeManagerAddress")
                break
        safe_ip = (node_ip or "unknown").replace(".", "-")
    except Exception:
        safe_ip = "unknown"

    # Include job id in actor name if provided to avoid collisions
    if job_or_submission_id:

        def _sanitize(s: str) -> str:
            return "".join(ch if (ch.isalnum() or ch in "-_.") else "-" for ch in s)[
                :48
            ]

        actor_name = f"rayssh_code_job_{safe_ip}_{_sanitize(job_or_submission_id)}"
    else:
        actor_name = f"rayssh_code_{safe_ip}"
    try:
        # Try to reuse an existing named actor first
        try:
            code_actor = ray.get_actor(actor_name, namespace="rayssh")
        except Exception:
            code_actor = None

        # Resolve root dir for code server
        # Priority:
        # - If job/submission specified: use '.' so actor uses its runtime_env working_dir
        # - Else if code_path given: use that path (Ray Client mode will upload)
        # - Else: default behavior
        if job_or_submission_id:
            root_dir = "."
        elif code_path:
            root_dir = code_path
        else:
            root_dir = None

        # If there is no existing named actor, use a probe actor to decide shipping
        if code_actor is None:
            # Create a short-lived probe actor to query state on the target node
            probe_actor = CodeServerActor.options(
                namespace="rayssh",
                scheduling_strategy=ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
                    node_id=worker_node_id, soft=False
                ),
            ).remote()

            try:
                try:
                    has_cs = ray.get(probe_actor.has_code_server.remote())
                except Exception:
                    has_cs = True

                if not has_cs:
                    print(
                        "⚙️  code-server not found on target. Downloading on client..."
                    )
                    # Ask target node for its platform info
                    try:
                        target_platform = ray.get(
                            probe_actor.get_platform_info.remote()
                        )
                        target_os = target_platform.get("os", "linux")
                        target_arch = target_platform.get("arch", "amd64")
                        print(f"🖥️  Target platform: {target_os}-{target_arch}")
                    except Exception as e:
                        print(
                            f"⚠️  Could not determine target platform, defaulting to linux-amd64: {e}"
                        )
                        target_os, target_arch = "linux", "amd64"

                    from utils import download_code_server_if_needed

                    code_server_archive = download_code_server_if_needed(
                        target_os, target_arch
                    )
                    if not code_server_archive:
                        print(
                            "❌ Failed to download code-server on client",
                            file=sys.stderr,
                        )
                        return 1

                    # Prefer Ray Client upload+install to avoid job body size limits
                    if not ray_address_env:
                        print(
                            "❌ RAY_ADDRESS is required to use Ray Client for shipping the archive",
                            file=sys.stderr,
                        )
                        return 1
                    ok = _install_codeserver_via_ray_client(
                        ray_address_env, worker_node_id, code_server_archive
                    )
                    if not ok:
                        return 1

                    # Now create the named detached actor without shipping (installed under ~/.rayssh)
                    actor_options = {
                        "name": actor_name,
                        "lifetime": "detached",
                        "namespace": "rayssh",
                        "scheduling_strategy": ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
                            node_id=worker_node_id, soft=False
                        ),
                    }
                    if n_gpus is not None:
                        actor_options["num_gpus"] = n_gpus

                    # Apply require constraints from environment
                    apply_require_constraints_to_actor_options(actor_options)

                    _apply_job_runtime_env_to_actor_options(
                        actor_options, job_or_submission_id
                    )
                    code_actor = CodeServerActor.options(**actor_options).remote()
                    print("✅ code-server installed and ready on target node")
                else:
                    # Create the named detached actor without shipping
                    actor_options = {
                        "name": actor_name,
                        "lifetime": "detached",
                        "namespace": "rayssh",
                        "scheduling_strategy": ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
                            node_id=worker_node_id, soft=False
                        ),
                    }
                    if n_gpus is not None:
                        actor_options["num_gpus"] = n_gpus

                    # Apply require constraints from environment
                    apply_require_constraints_to_actor_options(actor_options)

                    _apply_job_runtime_env_to_actor_options(
                        actor_options, job_or_submission_id
                    )
                    code_actor = CodeServerActor.options(**actor_options).remote()
            finally:
                try:
                    ray.kill(probe_actor)
                except Exception:
                    pass

        # Start code-server via the chosen actor
        print(f"🔧 Starting code-server...")
        if root_dir and not job_or_submission_id:
            print(f"📦 Uploading local folder `{root_dir}` to code-server...")
        result = ray.get(code_actor.start_code.remote(root_dir=root_dir, port=80))
        # print(f"[DEBUG] result: {result}")
        if not result.get("success"):
            err = str(result.get("error", ""))
            try:
                info = ray.get(code_actor.get_info.remote())
            except Exception:
                info = None
            if info and info.get("running") and info.get("url"):
                print("ℹ️  Existing code-server detected. Reusing running server:")
                BOLD = "\033[1m"
                RESET = "\033[0m"
                print(f"   🔗 {BOLD}{info['url']}{RESET}")
                if info.get("root_dir"):
                    folder_url = (
                        info["url"].rstrip("/")
                        + "/?"
                        + urllib.parse.urlencode({"folder": info["root_dir"]})
                    )
                    print(f"   📁 {folder_url}")
                if info.get("password"):
                    print(f"   🔐 PASSWORD: {BOLD}{info['password']}{RESET}")
                log_path = info.get("log_file")
                if log_path:
                    print(f"   📝 Log: {log_path}")
            else:
                print(f"❌ Failed to start code-server: {err}", file=sys.stderr)
                return 1
        else:
            actual_url = result.get("url")
            if actual_url:
                BOLD = "\033[1m"
                RESET = "\033[0m"
                # print the highlighted main URL
                print(f"   🔗 {BOLD}{actual_url}{RESET}")
                # Print folder link if root_dir resolved
                rd = result.get("root_dir")
                if rd:
                    folder_url = (
                        actual_url.rstrip("/")
                        + "/?"
                        + urllib.parse.urlencode({"folder": rd})
                    )
                    print(f"   📁 {folder_url}")
            if result.get("password"):
                print(f"   🔐 PASSWORD: {BOLD}{result['password']}{RESET}")
            # Persist last session preference (by IP)
            try:
                nodes, _ = fetch_cluster_nodes()
                for n in nodes:
                    if n.get("NodeID") == worker_node_id:
                        ip = n.get("NodeManagerAddress")
                        if ip:
                            write_last_session_node_ip(ip)
                        break
            except Exception:
                pass

        # Tail logs (show only key information)
        try:
            start_time = time.time()
            offset = 0
            seen_link = False
            access_url_shown = False
            while True:
                chunk = ray.get(code_actor.read_log_chunk.remote(offset, 65536))
                data = chunk.get("data", "")
                offset = chunk.get("next_offset", offset)
                if data:
                    for line in data.splitlines():
                        # Only show essential lines for code-server
                        if any(
                            keyword in line
                            for keyword in [
                                "HTTP server listening",  # final ready URL
                                "Using password",  # show password confirmation
                                "Error",
                                "ERROR",
                                "Failed",  # surface issues
                            ]
                        ):
                            if "HTTP server listening" in line:
                                seen_link = True
                                if not access_url_shown:
                                    print("✅ Server ready:")
                                    access_url_shown = True
                            print(f"   {line.strip()}")
                if quick and (seen_link or (time.time() - start_time) > 8.0):
                    print(
                        "✅ code-server launched. Exiting due to -q. Server continues running."
                    )
                    return 0
                time.sleep(0.5)
        except KeyboardInterrupt:
            if not quick:
                try:
                    ray.get(code_actor.stop.remote())
                    # Kill the actor to ensure it is fully terminated
                    try:
                        ray.kill(code_actor)
                    except Exception as kill_exc:
                        print(
                            f"⚠️  Failed to kill code-server actor: {kill_exc}",
                            file=sys.stderr,
                        )
                    print("\n🛑 Ctrl-C received. code-server terminated.")
                except Exception:
                    print(
                        "\n🛑 Ctrl-C received. Failed to stop code-server; it may still be running."
                    )
            else:
                print("\n🛑 Stopped tailing log. code-server continues to run.")
            return 0
    except Exception as e:
        print(f"Error creating CodeServerActor: {e}", file=sys.stderr)
        return 1
