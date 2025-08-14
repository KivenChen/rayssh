import os
import sys
import time
import random
from typing import List, Tuple, Dict, Optional

import ray

from lab_actor import LabActor
from code_server_actor import CodeServerActor
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

    # Initialize Ray
    ray_address_env = os.environ.get("RAY_ADDRESS")
    try:
        if ray_address_env:
            ensure_ray_initialized(ray_address=ray_address_env, working_dir=lab_path)
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

    # Placement
    try:
        worker_node_id = _select_worker_node_id(allow_head_if_no_worker)
    except Exception as e:
        print(f"Error selecting node: {e}", file=sys.stderr)
        return 1

    # Singleton LabActor per node
    actor_name = f"rayssh_lab_{worker_node_id}"
    try:
        try:
            lab_actor = ray.get_actor(actor_name, namespace="rayssh")
        except Exception:
            lab_actor = LabActor.options(
                name=actor_name,
                lifetime="detached",
                namespace="rayssh",
                scheduling_strategy=ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
                    node_id=worker_node_id, soft=False
                ),
            ).remote()
    except Exception as e:
        print(f"Error creating LabActor: {e}", file=sys.stderr)
        return 1

    # Resolve root dir
    if ray_address_env:
        root_dir = None if lab_path else os.path.expanduser("~")
    else:
        root_dir = os.path.expanduser("~")

    # Start lab
    result = ray.get(lab_actor.start_lab.remote(root_dir=root_dir, port=80))
    if not result.get("success"):
        err = str(result.get("error", ""))
        try:
            info = ray.get(lab_actor.get_info.remote())
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

    # Tail logs
    try:
        start_time = time.time()
        offset = 0
        seen_link = False
        while True:
            chunk = ray.get(lab_actor.read_log_chunk.remote(offset, 65536))
            data = chunk.get("data", "")
            offset = chunk.get("next_offset", offset)
            if data:
                for line in data.splitlines():
                    if "http://" in line or "https://" in line:
                        seen_link = True
                    print(line)
            if quick and (seen_link or (time.time() - start_time) > 8.0):
                print("✅ Lab launched. Exiting due to -q. Server continues running.")
                return 0
            time.sleep(0.5)
    except KeyboardInterrupt:
        if not quick:
            try:
                ray.get(lab_actor.stop.remote())
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

    ray_address_env = os.environ.get("RAY_ADDRESS")
    try:
        if ray_address_env:
            ensure_ray_initialized(ray_address=ray_address_env, working_dir=code_path)
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

    try:
        worker_node_id = _select_worker_node_id(allow_head_if_no_worker)
    except Exception as e:
        print(f"Error selecting node: {e}", file=sys.stderr)
        return 1

    actor_name = f"rayssh_code_{worker_node_id}"
    try:
        try:
            code_actor = ray.get_actor(actor_name, namespace="rayssh")
        except Exception:
            code_actor = CodeServerActor.options(
                name=actor_name,
                lifetime="detached",
                namespace="rayssh",
                scheduling_strategy=ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
                    node_id=worker_node_id, soft=False
                ),
            ).remote()
    except Exception as e:
        print(f"Error creating CodeServerActor: {e}", file=sys.stderr)
        return 1

    if ray_address_env:
        root_dir = None if code_path else os.path.expanduser("~")
    else:
        root_dir = os.path.expanduser("~")

    # Inform install
    try:
        has_cs = ray.get(code_actor.has_code_server.remote())
    except Exception:
        has_cs = True
    if not has_cs:
        print(
            "⚙️  code-server not found on target. Installing... (this may take a minute)"
        )

    result = ray.get(code_actor.start_code.remote(root_dir=root_dir, port=80))
    if not result.get("success"):
        err = str(result.get("error", ""))
        try:
            info = ray.get(code_actor.get_info.remote())
        except Exception:
            info = None
        if info and info.get("running") and info.get("url"):
            print("ℹ️  Existing code-server detected. Reusing running server:")
            print(f"🔗 {info['url']}")
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
            print(f"🔗 {actual_url}")
        if result.get("password"):
            print(f"🔐 PASSWORD: {result['password']}")

    # Tail logs
    try:
        start_time = time.time()
        offset = 0
        seen_link = False
        while True:
            chunk = ray.get(code_actor.read_log_chunk.remote(offset, 65536))
            data = chunk.get("data", "")
            offset = chunk.get("next_offset", offset)
            if data:
                for line in data.splitlines():
                    if "http://" in line or "https://" in line:
                        seen_link = True
                    print(line)
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
                print("\n🛑 Ctrl-C received. code-server terminated.")
            except Exception:
                print(
                    "\n🛑 Ctrl-C received. Failed to stop code-server; it may still be running."
                )
        else:
            print("\n🛑 Stopped tailing log. code-server continues to run.")
        return 0
