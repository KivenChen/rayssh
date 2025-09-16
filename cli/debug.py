#!/usr/bin/env python3
"""
Debug command handler for RaySSH CLI.
Provides code-server functionality with Ray debugging capabilities.
"""

import os
import sys
import time
import random
import subprocess
import urllib.parse
from typing import List, Optional

import ray

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
)
from ray.job_submission import JobSubmissionClient
from .code import (
    _install_codeserver_via_ray_client,
    _apply_job_runtime_env_to_actor_options,
)


def _select_worker_node_id(allow_head_if_no_worker: bool) -> str:
    """Select a worker node ID, same as in code.py"""
    # Ensure Ray is initialized prior to state API
    import os as _os
    import ray as _ray

    if not _ray.is_initialized():
        addr = _os.environ.get("RAY_ADDRESS")
        if addr:
            ensure_ray_initialized(ray_address=addr, working_dir=None)
        else:
            ensure_ray_initialized()
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


def handle_debug_command(argv: List[str]) -> int:
    """Handle the debug command - extends code command with Ray debugging features"""
    allow_head_if_no_worker = False
    args = argv
    if len(argv) >= 2 and argv[0] == "-0" and argv[1] == "debug":
        allow_head_if_no_worker = True
        args = argv[2:]
    elif len(argv) >= 1 and argv[0] == "debug":
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
        # Helper to resolve Ray-managed working_dir (gcs://...) on the target node
        def _resolve_workdir_on_target_node(
            wd_uri: str, target_node_id: str
        ) -> Optional[str]:
            try:
                if (
                    not wd_uri
                    or not isinstance(wd_uri, str)
                    or not wd_uri.startswith("gcs://")
                ):
                    return None
                from terminal.server import WorkdirActor

                actor = WorkdirActor.options(
                    runtime_env={"working_dir": wd_uri},
                    scheduling_strategy=ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
                        node_id=target_node_id, soft=False
                    ),
                ).remote()
                return ray.get(actor.get_working_dir.remote())
            except Exception:
                return None

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
            ensure_ray_initialized()

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

    # Get cluster info for dashboard URL from Ray context (prefers ray.init() address_info)
    dashboard_info = get_ray_dashboard_info()
    dashboard_url = dashboard_info.get("dashboard_url")
    cluster_host = dashboard_info.get("host")
    dashboard_port = dashboard_info.get("port")

    print(f"🐛 Starting debug mode with Ray debugging enabled")
    if dashboard_url:
        print(f"🔗 Ray dashboard: {dashboard_url}")
    elif dashboard_port and cluster_host:
        print(f"🔗 Ray dashboard: http://{cluster_host}:{dashboard_port}")

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

        actor_name = f"rayssh_debug_job_{safe_ip}_{_sanitize(job_or_submission_id)}"
    else:
        actor_name = f"rayssh_debug_{safe_ip}"
    try:
        # Try to reuse an existing named actor first
        try:
            code_actor = ray.get_actor(actor_name, namespace="rayssh")
        except Exception:
            code_actor = None

        # Resolve root dir for code server
        # Priority:
        # - If job/submission specified: let actor use its runtime_env working_dir (root_dir=None)
        # - Else if code_path given: use that path (Ray Client mode will upload)
        # - Else: default behavior → use CWD only for local clusters; for Ray Client, let actor pick HOME
        if job_or_submission_id:
            root_dir = None
        elif code_path:
            root_dir = code_path
        else:
            root_dir = os.getcwd() if not ray_address_env else None

        # If there is no existing named actor, use a probe actor to decide shipping
        resolved_root: Optional[str] = None
        if code_actor is None:
            # Import our custom debug-enabled CodeServerActor
            from agent.debug_code_server import DebugCodeServerActor

            # Create a short-lived probe actor to query state on the target node
            probe_actor = DebugCodeServerActor.options(
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

                    # Install code-server via Ray Client
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
                    # Inherit runtime_env from job if provided (working_dir/uris/env_vars)
                    inherit_res = _apply_job_runtime_env_to_actor_options(
                        actor_options, job_or_submission_id
                    )
                    # Resolve job's working_dir (if any) on the target node
                    rt = inherit_res.get("runtime_env") or {}
                    wd = rt.get("working_dir")
                    resolved_root = _resolve_workdir_on_target_node(wd, worker_node_id)
                    if job_or_submission_id and not inherit_res.get("success"):
                        print(
                            f"❌ Job/submission not found or runtime_env unavailable: {job_or_submission_id}",
                            file=sys.stderr,
                        )
                        return 1
                    code_actor = DebugCodeServerActor.options(**actor_options).remote()
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
                    inherit_res = _apply_job_runtime_env_to_actor_options(
                        actor_options, job_or_submission_id
                    )
                    if job_or_submission_id and not inherit_res.get("success"):
                        print(
                            f"❌ Job/submission not found or runtime_env unavailable: {job_or_submission_id}",
                            file=sys.stderr,
                        )
                        return 1
                    # Resolve job's working_dir (if any) on the target node
                    rt = inherit_res.get("runtime_env") or {}
                    wd = rt.get("working_dir")
                    resolved_root = _resolve_workdir_on_target_node(wd, worker_node_id)
                    code_actor = DebugCodeServerActor.options(**actor_options).remote()
            finally:
                try:
                    ray.kill(probe_actor)
                except Exception:
                    pass

        # Start code-server via the chosen actor with debug configuration
        print(f"🔧 Starting debug-enabled code-server...")
        if code_path and not job_or_submission_id and ray_address_env:
            print(f"📦 Uploading local folder `{root_dir}` to code-server...")

        # Pass debug configuration to the actor
        debug_config = {
            "ray_dashboard_url": dashboard_url,
            "ray_dashboard_host": cluster_host,
            "ray_dashboard_port": dashboard_port,
        }

        # If we resolved a Ray-managed working_dir on the target, use it; else use selected root_dir
        effective_root = resolved_root if resolved_root else root_dir

        result = ray.get(
            code_actor.start_debug_code.remote(
                root_dir=effective_root, port=80, debug_config=debug_config
            )
        )

        # Handle results similar to regular code command
        if not result.get("success"):
            err = str(result.get("error", ""))
            try:
                info = ray.get(code_actor.get_info.remote())
            except Exception:
                info = None
            if info and info.get("running") and info.get("url"):
                print("ℹ️  Existing debug code-server detected. Reusing running server:")
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
                print(f"❌ Failed to start debug code-server: {err}", file=sys.stderr)
                # Print extra diagnostics if present
                attempted_cmd = (
                    result.get("attempted_cmd") if isinstance(result, dict) else None
                )
                if attempted_cmd:
                    print(f"   🧪 Command: {attempted_cmd}")
                if result.get("port") and result.get("host_ip"):
                    print(f"   📍 Target: {result.get('host_ip')}:{result.get('port')}")
                if result.get("root_dir"):
                    print(f"   📁 Root: {result.get('root_dir')}")
                log_path = result.get("log_file") or (
                    info.get("log_file") if info else None
                )
                if log_path:
                    print(f"   📝 Log: {log_path}")
                    try:
                        # Tail last chunk of the log to surface immediate errors
                        st = os.stat(log_path)
                        start_off = max(0, st.st_size - 8192)
                        chunk = ray.get(
                            code_actor.read_log_chunk.remote(start_off, 8192)
                        )
                        data = (chunk or {}).get("data", "").rstrip()
                        if data:
                            print("   ── Log tail ──")
                            for ln in data.splitlines()[-50:]:
                                print(f"   {ln}")
                    except Exception:
                        pass
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

        print("🐛 Debug environment configured with:")
        print("   • RAY_DEBUG=1")
        print("   • RAY_POST_MORTEM_DEBUG=1")
        print("   • Python extension installed")
        if dashboard_port and cluster_host:
            print(f"   • Ray cluster configured: {cluster_host}:{dashboard_port}")

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
                                "extension",  # show extension installation
                                "Extension",
                            ]
                        ):
                            if "HTTP server listening" in line:
                                seen_link = True
                                if not access_url_shown:
                                    print("✅ Debug server ready:")
                                    access_url_shown = True
                            print(f"   {line.strip()}")
                if quick and (seen_link or (time.time() - start_time) > 15.0):
                    print(
                        "✅ debug code-server launched. Exiting due to -q. Server continues running."
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
                            f"⚠️  Failed to kill debug code-server actor: {kill_exc}",
                            file=sys.stderr,
                        )
                    print("\n🛑 Ctrl-C received. debug code-server terminated.")
                except Exception:
                    print(
                        "\n🛑 Ctrl-C received. Failed to stop debug code-server; it may still be running."
                    )
            else:
                print("\n🛑 Stopped tailing log. debug code-server continues to run.")
            return 0
    except Exception as e:
        print(f"Error creating DebugCodeServerActor: {e}", file=sys.stderr)
        return 1
