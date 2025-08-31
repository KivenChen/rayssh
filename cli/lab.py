#!/usr/bin/env python3
"""
Lab command handler for RaySSH CLI.
Provides Jupyter Lab functionality.
"""

import os
import sys
import time
import random
import subprocess
from typing import List, Optional

import ray

from agent.lab import LabActor
from utils import (
    ensure_ray_initialized,
    fetch_cluster_nodes,
    get_head_node_id,
    parse_n_gpus_from_env,
    load_last_session_preferred_ip,
    find_node_by_ip,
    write_last_session_node_ip,
)


def _select_worker_node_id(allow_head_if_no_worker: bool) -> str:
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

    # Placement - prefer last session IP if available
    try:
        prefer_ip = load_last_session_preferred_ip()
        worker_node_id = None
        if prefer_ip:
            node = find_node_by_ip(prefer_ip)
            if node and node.get("Alive"):
                worker_node_id = node.get("NodeID")
        if not worker_node_id:
            worker_node_id = _select_worker_node_id(allow_head_if_no_worker)
    except Exception as e:
        print(f"Error selecting node: {e}", file=sys.stderr)
        return 1

    # Parse GPU requirements from environment
    n_gpus = parse_n_gpus_from_env()
    if n_gpus is not None:
        print(f"🎛️ GPUs requested: {n_gpus}")

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
            # Already initialized above; no need to init again
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
        except Exception:
            # Modules are now available job-wide via ray.init runtime_env
            actor_options = {
                "name": actor_name,
                "lifetime": "detached",
                "namespace": "rayssh_lab",
                "scheduling_strategy": ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
                    node_id=worker_node_id, soft=False
                ),
            }
            if n_gpus is not None:
                actor_options["num_gpus"] = n_gpus
            cur_lab_actor = LabActor.options(**actor_options).remote()
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
