import ipaddress
import os
import re
from typing import Dict, List, Optional
from functools import lru_cache

import ray


def is_valid_ip(ip_str: str) -> bool:
    """Check if the given string is a valid IP address."""
    try:
        ipaddress.ip_address(ip_str)
        return True
    except ValueError:
        return False


def is_valid_node_id(node_id: str) -> bool:
    """Check if the given string is a valid Ray node ID format."""
    # Ray node IDs are typically hex strings, allow minimum 6 chars for prefix matching
    return bool(re.match(r'^[a-fA-F0-9]+$', node_id)) and len(node_id) >= 6


def parse_node_argument(node_arg: str) -> tuple[str, str]:
    """
    Parse the node argument to determine if it's an IP address or node ID.
    Returns (type, value) where type is either 'ip' or 'node_id'.
    """
    if is_valid_ip(node_arg):
        return ('ip', node_arg)
    elif is_valid_node_id(node_arg):
        return ('node_id', node_arg)
    else:
        raise ValueError(f"Invalid node argument: {node_arg}. \nUsage: rayssh <node|file> or rayssh -q <file>")


def get_ray_cluster_nodes() -> List[Dict]:
    """Get information about all nodes in the Ray cluster (alive only), normalized.

    Uses the State API helper to ensure a single source of truth.
    Assumes Ray has already been initialized by the caller.
    """
    nodes, _ = fetch_cluster_nodes_via_state()
    return nodes


def find_node_by_ip(target_ip: str) -> Optional[Dict]:
    """Find a Ray node by its IP address."""
    nodes, _ = fetch_cluster_nodes_via_state()

    for node in nodes:
        # Check both NodeManagerAddress and internal/external IPs
        node_ip = node.get('NodeManagerAddress')
        if node_ip == target_ip:
            return node

        # Also check if the target IP matches any of the node's addresses
        resources = node.get('Resources', {})
        if 'node:' + target_ip in resources:
            return node

    return None


def find_node_by_id(target_node_id: str) -> Optional[Dict]:
    """Find a Ray node by its node ID."""
    nodes, _ = fetch_cluster_nodes_via_state()

    for node in nodes:
        node_id = node.get('NodeID')
        if node_id and (node_id == target_node_id or node_id.startswith(target_node_id)):
            return node

    return None


def find_target_node(node_arg: str) -> Dict:
    """
    Find the target Ray node based on IP address or node ID.
    Returns the node information dict.
    """
    arg_type, value = parse_node_argument(node_arg)

    if arg_type == 'ip':
        node = find_node_by_ip(value)
        if not node:
            raise ValueError(f"No Ray node found with IP address: {value}")
    else:  # node_id
        node = find_node_by_id(value)
        if not node:
            raise ValueError(f"No Ray node found with node ID: {value}")

    # Check if node is alive
    if not node.get('Alive', False):
        raise ValueError(f"Target node is not alive: {value}")

    return node


def ensure_ray_initialized(ray_address: str = None, working_dir: str = None, connect_only: bool = False):
    """Ensure Ray is initialized, initialize if not.

    If connect_only is True, do not start a new local cluster. Attempt to connect
    to an existing cluster (local or remote) and raise if none is available.
    """
    if ray.is_initialized():
        return
    try:
        # Set environment variables to suppress Ray logging
        os.environ['RAY_DISABLE_IMPORT_WARNING'] = '1'
        os.environ['RAY_DEDUP_LOGS'] = '0'  # Reduce log deduplication overhead
        os.environ['GLOG_minloglevel'] = '3'  # Suppress glog messages (0=INFO, 1=WARNING, 2=ERROR, 3=FATAL)
        os.environ['GLOG_logtostderr'] = '0'  # Don't log to stderr
        os.environ['RAY_RAYLET_LOG_LEVEL'] = 'FATAL'  # Suppress raylet logs
        os.environ['RAY_CORE_WORKER_LOG_LEVEL'] = 'FATAL'  # Suppress core worker logs

        # Prepare runtime environment for Ray Client
        runtime_env = {}
        if working_dir and ray_address and ray_address.startswith('ray://'):
            runtime_env['working_dir'] = working_dir

        # Initialize Ray (either local cluster, connect-only, or Ray Client)
        if ray_address and ray_address.startswith('ray://'):
            # Ray Client connection
            ray.init(
                address=ray_address,
                runtime_env=runtime_env if runtime_env else None,
                logging_level='FATAL',
                log_to_driver=False,
                ignore_reinit_error=True
            )
        elif connect_only:
            # Connect to an existing Ray cluster without starting one
            # address="auto" will attempt to discover a running local cluster
            ray.init(
                address="auto",
                logging_level='FATAL',
                log_to_driver=False,
                include_dashboard=False,
                _temp_dir='/tmp/ray',
                configure_logging=False,
                ignore_reinit_error=True
            )
        else:
            # Local Ray cluster (may start one if none exists)
            ray.init(
                logging_level='FATAL',  # Only show fatal errors
                log_to_driver=False,    # Don't send raylet logs to driver
                include_dashboard=False, # Disable dashboard to reduce log noise
                _temp_dir='/tmp/ray',   # Use consistent temp directory
                configure_logging=False, # Don't configure Python logging
                ignore_reinit_error=True # Ignore reinitialization errors
            )
    except Exception as e:
        raise RuntimeError(f"Failed to initialize Ray: {e}") from e


def get_node_resources(node_info: Dict) -> Dict:
    """Extract useful resource information from node info."""
    resources = node_info.get('Resources', {})
    return {
        'CPU': resources.get('CPU', 0),
        'memory': resources.get('memory', 0),
        'node_id': node_info.get('NodeID', ''),
        'node_ip': node_info.get('NodeManagerAddress', ''),
        'alive': node_info.get('Alive', False)
    }


def get_head_node_id() -> Optional[str]:
    """Return the head node's NodeID using Ray State API filters.

    Assumes Ray has already been initialized by the caller.
    """
    try:
        from ray.util import state as ray_state  # type: ignore
        nodes = ray_state.list_nodes(filters=[("is_head_node", "=", True)])
        assert len(nodes) == 1, "There should be exactly one head node"
        head = nodes[0]
        # Support both object with attribute and dict-like
        node_id = getattr(head, "node_id", None)
        if not node_id and isinstance(head, dict):
            node_id = head.get("node_id") or head.get("NodeID")
        return node_id
    except Exception as e:
        print(f"Failed to determine head node via Ray State API: {e}")
        raise RuntimeError(f"Failed to determine head node via Ray State API: {e}")


@lru_cache(maxsize=1)
def fetch_cluster_nodes_via_state() -> tuple[list[Dict], Optional[str]]:
    """Fetch Ray nodes once via State API and return (normalized_nodes, head_node_id).

    - Assumes Ray has already been initialized by the caller.
    - Normalizes each node to keys: 'NodeID', 'NodeManagerAddress', 'Alive', 'Resources'.
    - Determines head_node_id from the same list by checking common flags.
    """
    from ray.util import state as ray_state  # type: ignore
    raw_nodes = ray_state.list_nodes(address="auto", detail=True)
    if not raw_nodes:
        return [], None

    head_node_id: Optional[str] = None
    for n in raw_nodes:
        # Object-like
        if getattr(n, "is_head_node", False) or getattr(n, "node_type", None) == "head":
            head_node_id = getattr(n, "node_id", None)
            break
        # Dict-like
        if isinstance(n, dict):
            if n.get("is_head_node") or n.get("node_type") == "head" or n.get("nodeType") == "head" or n.get("is_head") is True:
                head_node_id = n.get("node_id") or n.get("NodeID")
                break

    nodes: list[Dict] = []
    for n in raw_nodes:
        if hasattr(n, "to_dict"):
            d = n.to_dict()
        elif isinstance(n, dict):
            d = n
        else:
            d = {
                'NodeID': getattr(n, 'node_id', None),
                'NodeManagerAddress': getattr(n, 'node_ip', None),
                'Alive': getattr(n, 'state', '').upper() == 'ALIVE',
                'Resources': getattr(n, 'resources_total', {}) or {},
            }

        node_id = d.get('NodeID') or d.get('node_id')
        node_ip = d.get('NodeManagerAddress') or d.get('node_ip')
        state_alive = d.get('Alive') if 'Alive' in d else (str(d.get('state', '')).upper() == 'ALIVE')
        resources = d.get('Resources') or d.get('resources_total') or {}

        nodes.append({
            'NodeID': node_id,
            'NodeManagerAddress': node_ip,
            'Alive': bool(state_alive),
            'Resources': resources,
        })

    # Only return alive nodes
    nodes = [node for node in nodes if node.get('Alive', False)]
    return nodes, head_node_id
