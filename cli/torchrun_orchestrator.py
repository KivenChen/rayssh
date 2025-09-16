import os
import sys
import subprocess

import ray
from ray.util import get_node_ip_address
from ray.util.placement_group import (
    placement_group,
    remove_placement_group,
)
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy


@ray.remote
class Runner:
    def get_ip(self):
        return get_node_ip_address()

    def run(self, cmd, extra_env, rank):
        env = os.environ.copy()
        env.update({k: str(v) for k, v in extra_env.items()})
        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                env=env,
            )
            assert proc.stdout is not None
            for line in proc.stdout:
                try:
                    print(f"[node {rank}] {line}", end="")
                except Exception:
                    pass
            return proc.wait()
        except Exception as e:
            print(f"[node {rank}] Runner error: {e}", file=sys.stderr)
            return 1


def main():
    n_nodes = int(sys.argv[1])
    interpreter = sys.argv[2]
    file_path = sys.argv[3]
    per_node_gpus_arg = sys.argv[4] if len(sys.argv) > 4 else None
    master_port = int(sys.argv[5]) if len(sys.argv) > 5 else 29500

    ray.init(address="auto")

    # Determine per-node GPU requirement
    g = 0.0
    if per_node_gpus_arg and per_node_gpus_arg != "":
        g = max(0.0, float(per_node_gpus_arg))

    # Build STRICT_SPREAD placement group with one bundle per node
    bundles = []
    for _ in range(n_nodes):
        b = {"CPU": 1}
        if g > 0:
            b["GPU"] = g
        bundles.append(b)
    pg = placement_group(bundles, strategy="STRICT_SPREAD")
    ray.get(pg.ready())

    # Create actors per bundle
    actor_opts = {}
    if g > 0:
        actor_opts["num_gpus"] = g
    actors = []
    for i in range(n_nodes):
        strat = PlacementGroupSchedulingStrategy(
            placement_group=pg,
            placement_group_bundle_index=i,
            placement_group_capture_child_tasks=False,
        )
        a = Runner.options(scheduling_strategy=strat, **actor_opts).remote()
        actors.append(a)

    # First actor determines master address
    master_address = ray.get(actors[0].get_ip.remote())

    # Launch processes on each actor
    futures = []
    for rank, actor in enumerate(actors):
        envmap = {
            "N_NODES": n_nodes,
            "NODE_RANK": rank,
            "MASTER_ADDRESS": master_address,
            "MASTER_PORT": master_port,
        }
        cmd = [interpreter, file_path]
        futures.append(actor.run.remote(cmd, envmap, rank))

    # Collect results incrementally without timeouts using ray.wait
    pending = list(futures)
    results = []
    while pending:
        done, pending = ray.wait(pending, num_returns=1)
        results.extend(ray.get(done))

    rc = 0 if all(r == 0 for r in results) else 1
    try:
        remove_placement_group(pg)
    except Exception:
        pass
    sys.exit(rc)


if __name__ == "__main__":
    main()
