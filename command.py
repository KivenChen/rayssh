import os
import sys
import time
import random
import subprocess
import urllib.parse
from typing import List, Tuple, Dict, Optional

import ray

from agent.lab import LabActor
from agent.code_server import CodeServerActor
from utils import (
    ensure_ray_initialized,
    fetch_cluster_nodes_via_state,
    get_head_node_id,
)


def _select_worker_node_id(allow_head_if_no_worker: bool) -> str:
    nodes, head_node_id = fetch_cluster_nodes_via_state()
    if not nodes:
        raise RuntimeError("No alive Ray nodes found in the cluster")

    # Build worker-only list
    worker_node_ids: List[str] = []
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


def handle_lab_command(argv: List[str]) -> int:
    allow_head_if_no_worker = False
    args = argv
    # Accept forms: ['lab', ...] or ['-0','lab',...]
    if len(argv) >= 2 and argv[0] == "-0" and argv[1] == "lab":
        allow_head_if_no_worker = True
        args = argv[2:]
    elif len(argv) >= 1 and argv[0] == "lab":
        args = argv[1:]

    quick = False
    lab_path: Optional[str] = None
    for a in args:
        if a == "-q":
            quick = True
        else:
            lab_path = a

    # Placement - state API will initialize Ray automatically
    try:
        worker_node_id = _select_worker_node_id(allow_head_if_no_worker)
    except Exception as e:
        print(f"Error selecting node: {e}", file=sys.stderr)
        return 1

    # Initialize Ray (may already be initialized by state API, but ensure proper configuration)
    ray_address_env = os.environ.get("RAY_ADDRESS")
    try:
        if ray_address_env:
            # Only upload working_dir if user explicitly specified a path
            if lab_path:
                ensure_ray_initialized(
                    ray_address=ray_address_env, working_dir=lab_path
                )
            else:
                ensure_ray_initialized(ray_address=ray_address_env, working_dir=None)
        else:
            if lab_path:
                print(
                    "Warning: [path] is only used in remote mode (RAY_ADDRESS). Ignoring path."
                )
            ensure_ray_initialized()
    except Exception as e:
        msg = str(e)
        if "Version mismatch" in msg and "Python" in msg:
            print(
                "❌ Ray/Python version mismatch between this process and the running local cluster.",
                file=sys.stderr,
            )
            print(
                "   Fix: either (1) run 'rayssh' from the same Python env that started the cluster,",
                file=sys.stderr,
            )
            print(
                "        or (2) restart the local cluster from this env: 'ray stop' then 'ray start --head'",
                file=sys.stderr,
            )
        else:
            print(f"Error initializing Ray: {e}", file=sys.stderr)
        return 1

    # Singleton LabActor per node
    actor_name = f"rayssh_lab_on_worker_beta02_{worker_node_id}"
    try:
        try:
            cur_lab_actor = ray.get_actor(actor_name, namespace="rayssh_lab")
        except Exception as e:
            # Modules are now available job-wide via ray.init runtime_env
            cur_lab_actor = LabActor.options(
                name=actor_name,
                lifetime="detached",
                namespace="rayssh_lab",
                scheduling_strategy=ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
                    node_id=worker_node_id, soft=False
                ),
            ).remote()
    except Exception as e:
        print(f"Error creating LabActor: {e}", file=sys.stderr)
        return 1

    # Resolve root dir
    # - In remote mode with lab_path: upload and use the specified path
    # - In remote mode without lab_path: use remote cwd (None)
    # - In local cluster mode: always use remote cwd (None)
    if ray_address_env and lab_path:
        root_dir = lab_path  # Upload specified path in remote mode
    else:
        root_dir = None  # Use remote node's cwd

    # Start lab
    result = ray.get(cur_lab_actor.start_lab.remote(root_dir=root_dir, port=80))
    if not result.get("success"):
        err = str(result.get("error", ""))
        try:
            info = ray.get(cur_lab_actor.get_info.remote())
        except Exception:
            info = None
        if info and info.get("running") and info.get("url"):
            print("ℹ️  Existing Jupyter Lab detected. Reusing running server:")
            print(f"🔗 {info['url']}")
            log_path = info.get("log_file")
            if log_path:
                print(f"📝 Log: {log_path}")
        else:
            print(f"❌ Failed to start Jupyter Lab: {err}", file=sys.stderr)
            return 1
    else:
        actual_url = result.get("url")
        if actual_url:
            print(f"🔗 {actual_url}")

    # Tail logs (show only key information)
    try:
        start_time = time.time()
        offset = 0
        seen_link = False
        access_url_shown = False
        while True:
            chunk = ray.get(
                cur_lab_actor.read_log_chunk.remote(offset=offset, max_bytes=65536)
            )
            data = chunk.get("data", "")
            offset = chunk.get("next_offset", offset)
            if data:
                for line in data.splitlines():
                    # Only show important lines
                    if any(
                        keyword in line
                        for keyword in [
                            "Jupyter Server",
                            "running at:",
                            "http://",
                            "https://",
                            "Error",
                            "ERROR",
                            "Failed",
                            "token=",
                            "Or copy and paste",
                        ]
                    ):
                        # Clean up the URLs to show the actual accessible one
                        if "http://" in line and "token=" in line:
                            seen_link = True
                            if not access_url_shown:
                                print("🔗 Access URLs:")
                                access_url_shown = True
                        print(f"   {line.strip()}")
            if quick and (seen_link or (time.time() - start_time) > 8.0):
                print("✅ Lab launched. Exiting due to -q. Server continues running.")
                return 0
            time.sleep(0.5)
    except KeyboardInterrupt:
        if not quick:
            try:
                ray.get(cur_lab_actor.stop.remote())
                try:
                    ray.kill(cur_lab_actor)
                except Exception as kill_exc:
                    print(f"⚠️  Failed to kill Lab actor: {kill_exc}", file=sys.stderr)
                print("\n🛑 Ctrl-C received. Lab server terminated.")
            except Exception:
                print(
                    "\n🛑 Ctrl-C received. Failed to stop Lab server; it may still be running."
                )
        else:
            print("\n🛑 Stopped tailing log. Lab server continues to run.")
        return 0


def handle_code_command(argv: List[str]) -> int:
    allow_head_if_no_worker = False
    args = argv
    if len(argv) >= 2 and argv[0] == "-0" and argv[1] == "code":
        allow_head_if_no_worker = True
        args = argv[2:]
    elif len(argv) >= 1 and argv[0] == "code":
        args = argv[1:]

    quick = False
    code_path: Optional[str] = None
    for a in args:
        if a == "-q":
            quick = True
        else:
            code_path = a

    try:
        worker_node_id = _select_worker_node_id(allow_head_if_no_worker)
    except Exception as e:
        print(f"Error selecting node: {e}", file=sys.stderr)
        return 1

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
                    "Warning: [path] is only used in remote mode (RAY_ADDRESS). Ignoring path."
                )
            ensure_ray_initialized()
    except Exception as e:
        msg = str(e)
        if "Version mismatch" in msg and "Python" in msg:
            print(
                "❌ Ray/Python version mismatch between this process and the running local cluster.",
                file=sys.stderr,
            )
            print(
                "   Fix: either (1) run 'rayssh' from the same Python env that started the cluster,",
                file=sys.stderr,
            )
            print(
                "        or (2) restart the local cluster from this env: 'ray stop' then 'ray start --head'",
                file=sys.stderr,
            )
        else:
            print(f"Error initializing Ray: {e}", file=sys.stderr)
        return 1

    actor_name = f"rayssh_code_on_worker_beta08_{worker_node_id}"
    try:
        # Try to reuse an existing named actor first
        try:
            code_actor = ray.get_actor(actor_name, namespace="rayssh")
        except Exception:
            code_actor = None

        # Resolve root dir for code server
        # - With code_path: use the specified path
        # - Without code_path: open target user's home directory
        if code_path:
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
                    code_actor = CodeServerActor.options(
                        name=actor_name,
                        lifetime="detached",
                        namespace="rayssh",
                        scheduling_strategy=ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
                            node_id=worker_node_id, soft=False
                        ),
                    ).remote()
                    print("✅ code-server installed and ready on target node")
                else:
                    # Create the named detached actor without shipping
                    code_actor = CodeServerActor.options(
                        name=actor_name,
                        lifetime="detached",
                        namespace="rayssh",
                        scheduling_strategy=ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
                            node_id=worker_node_id, soft=False
                        ),
                    ).remote()
            finally:
                try:
                    ray.kill(probe_actor)
                except Exception:
                    pass

        # Start code-server via the chosen actor
        print(f"🔧 Starting code-server...")
        if root_dir:
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
                print(f"🔗 {info['url']}")
                if info.get("root_dir"):
                    folder_url = (
                        info["url"].rstrip("/")
                        + "/?"
                        + urllib.parse.urlencode({"folder": info["root_dir"]})
                    )
                    print(f"📁 {folder_url}")
                if info.get("password"):
                    print(f"🔐 PASSWORD: {info['password']}")
                log_path = info.get("log_file")
                if log_path:
                    print(f"📝 Log: {log_path}")
            else:
                print(f"❌ Failed to start code-server: {err}", file=sys.stderr)
                return 1
        else:
            actual_url = result.get("url")
            if actual_url:
                # print(f"🔗 {actual_url}")
                # Print folder link if root_dir resolved
                rd = result.get("root_dir")
                if rd:
                    folder_url = (
                        actual_url.rstrip("/")
                        + "/?"
                        + urllib.parse.urlencode({"folder": rd})
                    )
                    print(f"🔗 {folder_url}")
            if result.get("password"):
                print(f"🔐 PASSWORD: {result['password']}")

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


def get_ordered_nodes():
    """
    Get Ray nodes in the same order as displayed in --ls table.
    Returns (nodes, head_node_index) where nodes is list of alive nodes
    and head_node_index is the index of the head node.
    """
    # Use shared State API helper; assume Ray is already initialized by callers
    from utils import fetch_cluster_nodes_via_state

    nodes, head_node_id = fetch_cluster_nodes_via_state()
    if not nodes:
        return [], -1

    # Compute head index in this list (fallback to 0 if not found)
    head_node_index = 0
    if head_node_id:
        for i, node in enumerate(nodes):
            if node.get("NodeID") == head_node_id:
                head_node_index = i
                break

    return nodes, head_node_index


def get_node_by_index(index: int):
    """
    Get a node by its index in the --ls table.
    -0 = head node, -1 = first non-head node, etc.

    Returns the node dict or raises ValueError if index is invalid.
    """
    nodes, head_node_index = get_ordered_nodes()

    if not nodes:
        raise ValueError("No alive Ray nodes found in the cluster")

    if index == 0:
        # -0 means head node
        if head_node_index >= 0:
            return nodes[head_node_index]
        else:
            raise ValueError("No head node found")

    # For non-head nodes, create a list excluding the head node
    non_head_nodes = []
    for i, node in enumerate(nodes):
        if i != head_node_index:
            non_head_nodes.append(node)

    # -1 = first non-head, -2 = second non-head, etc.
    non_head_index = index - 1

    if non_head_index < 0 or non_head_index >= len(non_head_nodes):
        raise ValueError(
            f"Node index -{index} is out of range. Available: -0 to -{len(non_head_nodes)}"
        )

    return non_head_nodes[non_head_index]


def get_random_worker_node():
    """
    Get a random worker (non-head) node.
    Returns the node dict or None if no worker nodes are available.
    Raises ValueError if only head node exists.
    """
    nodes, head_node_index = get_ordered_nodes()

    if not nodes:
        raise ValueError("No alive Ray nodes found in the cluster")

    # Create list of worker nodes (non-head nodes)
    worker_nodes = []
    for i, node in enumerate(nodes):
        if i != head_node_index:
            worker_nodes.append(node)

    if not worker_nodes:
        raise ValueError(
            "Only head node available. Use 'rayssh -0' to connect to head node, or 'rayssh -l' to see all nodes."
        )

    # Randomly select a worker node
    selected_node = random.choice(worker_nodes)
    return selected_node


def print_nodes_table():
    """Print a table of available Ray nodes."""
    try:
        # Fetch and normalize using shared helper
        from utils import fetch_cluster_nodes_via_state

        nodes, head_node_id = fetch_cluster_nodes_via_state()
        if not nodes:
            print("🚫 No Ray nodes found in the cluster.")
            return 0

        print(f"🌐 Ray Cluster Nodes ({len(nodes)} alive)")
        print("=" * 85)

        # Header
        print(
            f"{'📍 Node':<12} {'🌍 IP Address':<16} {'🆔 ID':<8} {'🖥️  CPU':<12} {'🎮 GPU':<12} {'💾 Memory':<12}"
        )
        print("-" * 85)

        # Print rows
        for i, node in enumerate(nodes, 1):
            node_ip = node.get("NodeManagerAddress", "N/A") or "N/A"
            full_node_id = node.get("NodeID", "N/A") or "N/A"
            node_id_short = f"{full_node_id[:6]}..." if full_node_id != "N/A" else "N/A"

            is_head_node = full_node_id == head_node_id
            node_label = f"{'👑 Head' if is_head_node else str(i)}"

            resources = node.get("Resources", {}) or {}
            cpu_total = int(resources.get("CPU", 0)) if resources.get("CPU", 0) else 0
            gpu_total = int(resources.get("GPU", 0)) if resources.get("GPU", 0) else 0

            # memory reported may be bytes; format to GB
            memory_val = resources.get("memory", 0) or 0
            try:
                memory_gb = (
                    f"{(float(memory_val) / (1024**3)):.1f}GB"
                    if float(memory_val) > 0
                    else "0GB"
                )
            except Exception:
                memory_gb = "0GB"

            print(
                f"{node_label:<12} {node_ip:<16} {node_id_short:<8} {cpu_total:<12} {gpu_total:<12} {memory_gb:<12}"
            )

            # Special resources
            special_resources = []
            for key, value in sorted(resources.items()):
                if key not in [
                    "CPU",
                    "GPU",
                    "memory",
                    "object_store_memory",
                ] and not key.startswith("node:"):
                    if "accelerator" in key.lower() or "tpu" in key.lower():
                        special_resources.append(
                            f"⚡ {key}: {int(value) if isinstance(value, float) and value.is_integer() else value}"
                        )
                    else:
                        special_resources.append(
                            f"🔧 {key}: {int(value) if isinstance(value, float) and value.is_integer() else value}"
                        )
            if special_resources:
                print(f"{'':12} {'└─':16} {', '.join(special_resources)}")

        print("=" * 85)
        print(
            "💡 Use: rayssh <ip_address> or rayssh <node_id_prefix> or rayssh -<index> to connect"
        )
        print("👑 Head node is -0, first non-head is -1, second non-head is -2, etc.")
        print(
            "🎲 Use 'rayssh' (no args) for random worker, 'rayssh -l' for interactive, 'rayssh --ls' for this table"
        )

    except Exception as e:
        print(f"❌ Error listing nodes: {e}", file=sys.stderr)
        return 1

    return 0


def interactive_node_selector():
    """
    Display an interactive node selection screen with arrow key navigation.
    Returns the selected node's IP address or None if cancelled.
    """
    try:
        # Get nodes in the same order as --ls table (via State API helper)
        nodes, head_node_index = get_ordered_nodes()

        if not nodes:
            print("🚫 No Ray nodes found in the cluster.")
            return None

        # Create display list with proper indexing
        display_nodes = []
        for i, node in enumerate(nodes):
            node_ip = node.get("NodeManagerAddress", "N/A")
            node_id = node.get("NodeID", "N/A")[:6]
            is_head = i == head_node_index

            # Get resources for display
            resources = node.get("Resources", {})
            cpu = int(resources.get("CPU", 0)) if resources.get("CPU", 0) else 0
            gpu = int(resources.get("GPU", 0)) if resources.get("GPU", 0) else 0
            memory_gb = (
                resources.get("memory", 0) / (1024**3) if resources.get("memory") else 0
            )

            # Get special resources (accelerators, etc.)
            special_resources = []
            for key, value in sorted(resources.items()):
                if key not in [
                    "CPU",
                    "GPU",
                    "memory",
                    "object_store_memory",
                ] and not key.startswith("node:"):
                    if "accelerator" in key.lower() or "tpu" in key.lower():
                        special_resources.append(
                            f"{key}: {int(value) if isinstance(value, float) and value.is_integer() else value}"
                        )

            display_nodes.append(
                {
                    "node": node,
                    "ip": node_ip,
                    "id": node_id,
                    "is_head": is_head,
                    "cpu": cpu,
                    "gpu": gpu,
                    "memory": f"{memory_gb:.1f}GB",
                    "special": ", ".join(special_resources)
                    if special_resources
                    else "",
                    "index": 0
                    if is_head
                    else len(
                        [n for j, n in enumerate(nodes[:i]) if j != head_node_index]
                    )
                    + 1,
                }
            )

        # Simple numbered selection instead of arrow keys
        os.system("clear")
        print("🎯 RaySSH: Node Selection")
        print("=" * 50)
        print()

        # Display nodes as a numbered list with 0-based indexing matching command line
        print("    #    Type     IP Address       ID       CPU    GPU    Memory")
        print("-" * 60)

        for i, display_node in enumerate(display_nodes):
            # Use 0 for head node, then 1, 2, 3... for non-head nodes
            if display_node["is_head"]:
                node_number = 0
                node_type = "HEAD"
            else:
                node_number = display_node[
                    "index"
                ]  # This already has the correct -1, -2, -3... logic
                node_type = f"-{display_node['index']}"

            print(
                f"    {node_number:<4} {node_type:<8} {display_node['ip']:<16} {display_node['id']:<8} {display_node['cpu']:<6} {display_node['gpu']:<6} {display_node['memory']:<8}"
            )

            # Show special resources if they exist
            if display_node["special"]:
                print(f"         └─ {display_node['special']}")

        print()
        print("=" * 50)
        print("🔢 Enter node number to connect (0=head, 1+=non-head, 'q' to cancel):")

        try:
            choice = input("> ").strip()
            if choice.lower() in ["q", "quit", "exit"]:
                return None
            if choice == "":
                return None

            node_num = int(choice)

            # Find the node with the matching number
            selected_node = None
            for display_node in display_nodes:
                if display_node["is_head"] and node_num == 0:
                    selected_node = display_node
                    break
                elif not display_node["is_head"] and node_num == display_node["index"]:
                    selected_node = display_node
                    break

            if selected_node:
                print(f"🚀 Connecting to {selected_node['ip']}...")
                return selected_node["ip"]
            else:
                # Show available numbers for error message
                available_nums = []
                for display_node in display_nodes:
                    if display_node["is_head"]:
                        available_nums.append("0")
                    else:
                        available_nums.append(str(display_node["index"]))
                print(
                    f"❌ Invalid selection. Available: {', '.join(available_nums)} or 'q' to cancel."
                )
                return None

        except (ValueError, KeyboardInterrupt):
            return None

    except Exception as e:
        print(f"❌ Error in node selector: {e}", file=sys.stderr)
        return None



def submit_file_job(file_path: str, no_wait: bool = False) -> int:
    """
    Submit a file as a Ray job (experimental feature).

    Args:
        file_path: Path to the file to execute
        no_wait: If True, don't wait for job completion

    Returns:
        Exit code (0 for success, 1 for error)
    """
    try:
        # Ensure Ray is initialized
        ensure_ray_initialized()

        # Validate file path restrictions
        if not os.path.exists(file_path):
            print(f"Error: File '{file_path}' not found", file=sys.stderr)
            return 1

        # Check if file is within current working directory
        abs_file_path = os.path.abspath(file_path)
        abs_cwd = os.path.abspath(".")

        if not abs_file_path.startswith(abs_cwd):
            print(
                "Error: File must be within current working directory (experimental restriction)",
                file=sys.stderr,
            )
            return 1

        # Check if it's a text file (not binary)
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                f.read(1024)  # Read first 1KB to check if it's text
        except UnicodeDecodeError:
            print(
                "Error: Binary files are not supported (experimental restriction)",
                file=sys.stderr,
            )
            return 1

        # Determine the interpreter based on file extension
        file_extension = os.path.splitext(file_path)[1].lower()
        if file_extension == ".py":
            interpreter = "python"
        elif file_extension in [".sh", ".bash"]:
            interpreter = "bash"
        else:
            # Try to detect shebang
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    first_line = f.readline().strip()
                    if first_line.startswith("#!"):
                        if "python" in first_line:
                            interpreter = "python"
                        elif "bash" in first_line or "sh" in first_line:
                            interpreter = "bash"
                        else:
                            print(
                                "Error: Unsupported interpreter in shebang. Only Python and Bash are supported.",
                                file=sys.stderr,
                            )
                            return 1
                    else:
                        print(
                            "Error: Cannot determine interpreter. Use .py or .sh/.bash extension, or add shebang.",
                            file=sys.stderr,
                        )
                        return 1
            except Exception as e:
                print(f"Error reading file: {e}", file=sys.stderr)
                return 1

        # Prepare working dir and runtime env options
        working_dir_opt = "--working-dir=."
        runtime_env_candidates = ["runtime_env.yaml", "runtime_env.yml"]
        runtime_env_file = next(
            (f for f in runtime_env_candidates if os.path.isfile(f)), None
        )
        runtime_env_present = runtime_env_file is not None

        # Parse GPUs from environment variable (supports 'n_gpus' or 'N_GPUS')
        gpu_env = os.environ.get("n_gpus") or os.environ.get("N_GPUS")
        entrypoint_num_gpus_arg = None
        gpu_str = None
        if gpu_env is not None and gpu_env != "":
            try:
                gpu_count = float(gpu_env)
                if gpu_count < 0:
                    raise ValueError
                # Use integer formatting if whole number, else keep float
                gpu_str = (
                    str(int(gpu_count)) if gpu_count.is_integer() else str(gpu_count)
                )
                entrypoint_num_gpus_arg = f"--entrypoint-num-gpus={gpu_str}"
            except Exception:
                print(
                    f"Error: Invalid n_gpus value '{gpu_env}'. Must be a number >= 0.",
                    file=sys.stderr,
                )
                return 1

        # Build Ray job submit command
        cmd = [
            "ray",
            "job",
            "submit",
            "--entrypoint-num-cpus=1",
            working_dir_opt,
        ]

        if entrypoint_num_gpus_arg:
            cmd.append(entrypoint_num_gpus_arg)

        if runtime_env_present:
            cmd.append(f"--runtime-env={runtime_env_file}")

        cmd.append("--")

        if no_wait:
            # Insert no-wait just before entrypoint
            cmd.insert(cmd.index("--"), "--no-wait")

        cmd.extend([interpreter, file_path])

        # Print concise context
        print(f"🚀 RaySSH: Submitting {interpreter} job: {file_path}")
        print(f"📦 Working dir: .")
        if runtime_env_present:
            print(f"🧩 Runtime env: ./{runtime_env_file}")
        else:
            print(
                f"🧩 Runtime env: remote (create runtime_env.yaml or runtime_env.yml to customize)"
            )
        if gpu_str is not None:
            print(f"🎛️ GPUs: {gpu_str}")
        print(f"📋 Command: {' '.join(cmd)}")
        print("⚠️  Experimental feature - file execution via Ray job submission")
        print()

        # Execute the ray job submit command
        result = subprocess.run(cmd, cwd=".")
        return result.returncode

    except Exception as e:
        print(f"Error submitting job: {e}", file=sys.stderr)
        return 1
