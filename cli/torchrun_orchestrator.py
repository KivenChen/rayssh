import os
import sys
import subprocess

import ray
from ray.util import get_node_ip_address
import logging
from ray.util.placement_group import (
    placement_group,
    remove_placement_group,
)
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

# Import utils for require constraints parsing
import utils

logger = logging.getLogger("ray")


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
            logger.info(f"[node {rank}] Running command: {cmd}, pid: {proc.pid}")
            for line in proc.stdout:
                print(f"[node {rank}] {line}", end="")
            return proc.wait()
        except Exception as e:
            logger.error(f"[node {rank}] RaySSH multi-node runner error: {e}")
            return 1


def main():
    n_nodes = int(sys.argv[1])
    interpreter = sys.argv[2]
    file_path = sys.argv[3]
    per_node_gpus_arg = sys.argv[4] if len(sys.argv) > 4 else None
    master_port = int(sys.argv[5]) if len(sys.argv) > 5 else 29500

    # NOTE(kiv): we need to log to driver to see all nodes' logs
    # however, it will cause logs on the driver node to be duplicated (only driver's node)
    # we decided not to "fix" it for now, as it obeys how fd works in Linux,
    # and Ray has log deduplication by default as well.
    ray.init(address="auto", log_to_driver=True)

    # Determine per-node GPU requirement
    g = 0.0
    if per_node_gpus_arg and per_node_gpus_arg != "":
        g = max(0.0, float(per_node_gpus_arg))

    # Build STRICT_SPREAD placement group with one bundle per node
    bundles = []

    # Get require constraints from environment
    require_constraints = utils.parse_require_constraints_from_env()
    if require_constraints:
        print(f"🎯 Applied resource constraints to placement group: {require_constraints}")

    for _ in range(n_nodes):
        b = {"CPU": 1}
        if g > 0:
            b["GPU"] = g
        # Add require constraints to each bundle
        b.update(require_constraints)
        bundles.append(b)
    pg = placement_group(bundles, strategy="STRICT_SPREAD")
    ray.get(pg.ready())

    # Create actors per bundle
    actor_opts = {}
    if g > 0:
        actor_opts["num_gpus"] = g

    # Apply require constraints to actor options
    if require_constraints:
        if "resources" not in actor_opts:
            actor_opts["resources"] = {}
        actor_opts["resources"].update(require_constraints)
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
        logger.info(f"Launched process on node {rank}, actor: {actor}")

    # Collect results incrementally without timeouts using ray.wait
    results = ray.get(list(futures))

    rc = 0 if all(r == 0 for r in results) else 1
    try:
        remove_placement_group(pg)
    except Exception:
        pass
    sys.exit(rc)


if __name__ == "__main__":
    main()
