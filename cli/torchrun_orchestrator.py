import os
import sys
import subprocess
from typing import Dict

import ray
from ray.util import get_node_ip_address
import logging
from ray.util.placement_group import (
    placement_group,
    remove_placement_group,
)
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

# Import utils for require constraints parsing

logger = logging.getLogger("ray")


# NOTE(kiv): this is a copy from utils.py to reduce dependency on RaySSH
def parse_require_constraints_from_env() -> Dict[str, float]:
    """Parse require constraints from environment variable.

    Parses the 'require' or 'requires' environment variable in format:
    'accelerator_type:H20,some_other_resource=0.0001'

    Environment variables checked (in order):
    - require, REQUIRE, requires, REQUIRES

    Rules:
    - Comma-separated resource specifications
    - If no '=<number>' is specified, defaults to 0.0001
    - Supports both ':' and '=' as separators for resource names

    Returns:
        Dict mapping resource names to quantities for Ray actor options

    Examples:
        require='accelerator_type:H20' -> {'accelerator_type:H20': 0.0001}
        requires='memory_type:HBM=2.5,accelerator_type:V100' ->
            {'memory_type:HBM': 2.5, 'accelerator_type:V100': 0.0001}
    """
    require_env = (
        os.environ.get("require")
        or os.environ.get("REQUIRE")
        or os.environ.get("requires")
        or os.environ.get("REQUIRES")
    )
    if not require_env or require_env.strip() == "":
        return {}

    constraints = {}
    try:
        # Split by comma to get individual resource specs
        specs = [spec.strip() for spec in require_env.split(",") if spec.strip()]

        for spec in specs:
            if "=" in spec:
                # Format: resource_name=quantity
                resource_name, quantity_str = spec.split("=", 1)
                resource_name = resource_name.strip()
                try:
                    quantity = float(quantity_str.strip())
                    if quantity < 0:
                        print(
                            f"⚠️  Warning: Negative quantity {quantity} for resource '{resource_name}', using 0.0001"
                        )
                        quantity = 0.0001
                except ValueError:
                    print(
                        f"⚠️  Warning: Invalid quantity '{quantity_str}' for resource '{resource_name}', using 0.0001"
                    )
                    quantity = 0.0001
            else:
                # Format: resource_name (use default quantity)
                resource_name = spec.strip()
                quantity = 0.0001

            if resource_name:
                constraints[resource_name] = quantity

    except Exception as e:
        print(f"⚠️  Warning: Failed to parse require constraints '{require_env}': {e}")
        return {}

    return constraints


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
    require_constraints = parse_require_constraints_from_env()
    if require_constraints:
        print(
            f"🎯 Applied resource constraints to placement group: {require_constraints}"
        )

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
    actor_ips = []
    for i in range(n_nodes):
        strat = PlacementGroupSchedulingStrategy(
            placement_group=pg,
            placement_group_bundle_index=i,
            placement_group_capture_child_tasks=False,
        )
        a = Runner.options(scheduling_strategy=strat, **actor_opts).remote()
        actors.append(a)
        actor_ips.append(ray.get(a.get_ip.remote()))

    # First actor determines master address
    master_address = actor_ips[0]

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
        logger.info(
            f"Launched process on node #{rank}, ip: {actor_ips[rank]}, actor: {actor}"
        )
    logger.info(f"Launched processes on {n_nodes} nodes:")
    logger.info(f"Master address: {master_address}")
    logger.info(f"Master port: {master_port}")
    logger.info(f"Node IPs: {actor_ips}")
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
