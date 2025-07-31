import ray
import re
import socket
import ipaddress
import os
import sys
import subprocess
import signal
from typing import Optional, Dict, List


def is_valid_ip(ip_str: str) -> bool:
    """Check if the given string is a valid IP address."""
    try:
        ipaddress.ip_address(ip_str)
        return True
    except ValueError:
        return False


def is_valid_node_id(node_id: str) -> bool:
    """Check if the given string is a valid Ray node ID format."""
    # Ray node IDs are typically hex strings
    return bool(re.match(r'^[a-fA-F0-9]+$', node_id)) and len(node_id) >= 8


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
        raise ValueError(f"Invalid node argument: {node_arg}. Must be a valid IP address or node ID.")


def get_ray_cluster_nodes() -> List[Dict]:
    """Get information about all nodes in the Ray cluster."""
    try:
        return ray.nodes()
    except Exception as e:
        raise RuntimeError(f"Failed to get Ray cluster nodes: {e}")


def find_node_by_ip(target_ip: str) -> Optional[Dict]:
    """Find a Ray node by its IP address."""
    nodes = get_ray_cluster_nodes()
    
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
    nodes = get_ray_cluster_nodes()
    
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


def ensure_ray_initialized():
    """Ensure Ray is initialized, initialize if not."""
    if not ray.is_initialized():
        try:
            # Set environment variables to suppress Ray logging
            os.environ['RAY_DISABLE_IMPORT_WARNING'] = '1'
            os.environ['RAY_DEDUP_LOGS'] = '0'  # Reduce log deduplication overhead
            os.environ['GLOG_minloglevel'] = '3'  # Suppress glog messages (0=INFO, 1=WARNING, 2=ERROR, 3=FATAL)
            os.environ['GLOG_logtostderr'] = '0'  # Don't log to stderr
            os.environ['RAY_RAYLET_LOG_LEVEL'] = 'FATAL'  # Suppress raylet logs
            os.environ['RAY_CORE_WORKER_LOG_LEVEL'] = 'FATAL'  # Suppress core worker logs
            
            ray_address = os.environ.get('RAY_ADDRESS')
            
            # If we have a RAY_ADDRESS, try different connection strategies
            if ray_address:
                success = False
                
                # Strategy 1: Try direct connection with timeout (original behavior)
                print("Attempting to connect to Ray cluster...")
                try:
                    def timeout_handler(signum, frame):
                        raise TimeoutError("Ray initialization timed out")
                    
                    # Set a 10 second timeout for ray.init()
                    signal.signal(signal.SIGALRM, timeout_handler)
                    signal.alarm(10)
                    
                    ray.init(
                        logging_level='FATAL',
                        log_to_driver=False,
                        include_dashboard=False,
                        _temp_dir='/tmp/ray',
                        configure_logging=False,
                        ignore_reinit_error=True
                    )
                    signal.alarm(0)  # Cancel the alarm
                    success = True
                    print("✅ Connected to Ray cluster via direct connection")
                    
                except (TimeoutError, Exception) as e:
                    signal.alarm(0)  # Cancel the alarm
                    print(f"⚠️  Direct connection failed: {e}")
                    
                    if ray.is_initialized():
                        ray.shutdown()
                    
                    # Strategy 2: Try Ray Client mode if address doesn't have ray:// prefix
                    if not ray_address.startswith('ray://'):
                        print("Trying Ray Client mode...")
                        try:
                            # Extract host from address
                            host = ray_address.split(':')[0]
                            client_address = f"ray://{host}:10001"  # Standard Ray Client port
                            
                            # Temporarily set RAY_ADDRESS for ray client
                            original_address = os.environ.get('RAY_ADDRESS')
                            os.environ['RAY_ADDRESS'] = client_address
                            
                            ray.init(
                                logging_level='FATAL',
                                log_to_driver=False,
                                include_dashboard=False,
                                configure_logging=False,
                                ignore_reinit_error=True
                            )
                            success = True
                            print("✅ Connected to Ray cluster via Ray Client")
                            
                        except Exception as client_e:
                            print(f"⚠️  Ray Client connection failed: {client_e}")
                            if ray.is_initialized():
                                ray.shutdown()
                            
                            # Restore original address
                            if original_address:
                                os.environ['RAY_ADDRESS'] = original_address
                            else:
                                os.environ.pop('RAY_ADDRESS', None)
                
                if not success:
                    # Strategy 3: Start local cluster as fallback
                    print("Falling back to local Ray cluster...")
                    # Temporarily remove RAY_ADDRESS to force local cluster
                    original_address = os.environ.pop('RAY_ADDRESS', None)
                    try:
                        # Ensure Ray is completely shutdown before starting local cluster
                        try:
                            ray.shutdown()
                        except:
                            pass
                        
                        # Clear any Ray environment variables that might interfere
                        for key in list(os.environ.keys()):
                            if key.startswith('RAY_'):
                                if key not in ['RAY_DISABLE_IMPORT_WARNING', 'RAY_DEDUP_LOGS', 'RAY_RAYLET_LOG_LEVEL', 'RAY_CORE_WORKER_LOG_LEVEL']:
                                    os.environ.pop(key, None)
                        
                        ray.init(
                            logging_level='FATAL',
                            log_to_driver=False,
                            include_dashboard=False,
                            _temp_dir='/tmp/ray',
                            configure_logging=False,
                            ignore_reinit_error=True
                        )
                        print("✅ Started local Ray cluster")
                        print("⚠️  Note: Using local cluster instead of remote cluster")
                    except Exception as local_e:
                        # Restore original address
                        if original_address:
                            os.environ['RAY_ADDRESS'] = original_address
                        raise RuntimeError(f"Failed to initialize Ray with local cluster: {local_e}")
            else:
                # No RAY_ADDRESS set, use original local initialization
                ray.init(
                    logging_level='FATAL',
                    log_to_driver=False,
                    include_dashboard=False,
                    _temp_dir='/tmp/ray',
                    configure_logging=False,
                    ignore_reinit_error=True
                )
                
        except Exception as e:
            raise RuntimeError(f"Failed to initialize Ray: {e}")


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
