import ipaddress
import os
import re
from typing import Dict, List, Optional
import platform
import subprocess
import re
from functools import lru_cache
import socket

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
    return bool(re.match(r"^[a-fA-F0-9]+$", node_id)) and len(node_id) >= 6


def parse_node_argument(node_arg: str) -> tuple[str, str]:
    """
    Parse the node argument to determine if it's an IP address or node ID.
    Returns (type, value) where type is either 'ip' or 'node_id'.
    """
    # Handle special cases like localhost
    if node_arg.lower() in ["localhost", "127.0.0.1", "::1"]:
        return ("ip", "127.0.0.1")  # Normalize to 127.0.0.1
    elif is_valid_ip(node_arg):
        return ("ip", node_arg)
    elif is_valid_node_id(node_arg):
        return ("node_id", node_arg)
    else:
        raise ValueError(
            f"Invalid node argument: {node_arg}. \nUsage: rayssh <node|file> or rayssh -q <file>"
        )


def get_ray_cluster_nodes() -> List[Dict]:
    """Get information about all nodes in the Ray cluster (alive only), normalized.

    Uses Ray's public nodes() API via our helper.
    Assumes Ray has already been initialized by the caller.
    """
    nodes, _ = fetch_cluster_nodes()
    return nodes


def find_node_by_ip(target_ip: str) -> Optional[Dict]:
    """Find a Ray node by its IP address."""
    nodes, _ = fetch_cluster_nodes()

    for node in nodes:
        # Check both NodeManagerAddress and internal/external IPs
        node_ip = node.get("NodeManagerAddress")
        if node_ip == target_ip:
            return node

        # Also check if the target IP matches any of the node's addresses
        resources = node.get("Resources", {})
        if "node:" + target_ip in resources:
            return node

    return None


def find_node_by_id(target_node_id: str) -> Optional[Dict]:
    """Find a Ray node by its node ID."""
    nodes, _ = fetch_cluster_nodes()

    for node in nodes:
        node_id = node.get("NodeID")
        if node_id and (
            node_id == target_node_id or node_id.startswith(target_node_id)
        ):
            return node

    return None


def find_target_node(node_arg: str) -> Dict:
    """
    Find the target Ray node based on IP address or node ID.
    Returns the node information dict.
    """
    arg_type, value = parse_node_argument(node_arg)

    if arg_type == "ip":
        node = find_node_by_ip(value)
        if not node:
            raise ValueError(f"No Ray node found with IP address: {value}")
    else:  # node_id
        node = find_node_by_id(value)
        if not node:
            raise ValueError(f"No Ray node found with node ID: {value}")

    # Check if node is alive
    if not node.get("Alive", False):
        raise ValueError(f"Target node is not alive: {value}")

    return node


def ensure_ray_initialized(
    ray_address: str = None, working_dir: str = None, connect_only: bool = False
):
    """Ensure Ray is initialized, initialize if not.

    If connect_only is True, do not start a new local cluster. Attempt to connect
    to an existing cluster (local or remote) and raise if none is available.
    """
    if ray.is_initialized():
        return
    try:
        # Set environment variables to suppress Ray logging
        os.environ["RAY_DISABLE_IMPORT_WARNING"] = "1"
        os.environ["RAY_DEDUP_LOGS"] = "0"  # Reduce log deduplication overhead
        os.environ["GLOG_minloglevel"] = (
            "3"  # Suppress glog messages (0=INFO, 1=WARNING, 2=ERROR, 3=FATAL)
        )
        os.environ["GLOG_logtostderr"] = "0"  # Don't log to stderr
        os.environ["RAY_RAYLET_LOG_LEVEL"] = "FATAL"  # Suppress raylet logs
        os.environ["RAY_CORE_WORKER_LOG_LEVEL"] = "FATAL"  # Suppress core worker logs

        # Suppress additional Ray client and worker messages
        import logging

        logging.getLogger("ray").setLevel(logging.WARNING)
        logging.getLogger("ray.serve").setLevel(logging.WARNING)
        logging.getLogger("ray.tune").setLevel(logging.WARNING)
        logging.getLogger("ray.rllib").setLevel(logging.WARNING)
        logging.getLogger("ray.workflow").setLevel(logging.WARNING)

        # Prepare runtime environment for Ray Client
        runtime_env = {}
        if working_dir and ray_address and ray_address.startswith("ray://"):
            runtime_env["working_dir"] = working_dir

        # Add required modules for RaySSH actors
        try:
            # Ship the entire project root so remote workers import the same code
            import agent
            import terminal
            import utils
            runtime_env["py_modules"] = [agent, terminal, utils]
        except ImportError as e:
            # If modules can't be imported, continue without py_modules
            print(f"Error importing modules: {e}")
            pass

        # Initialize Ray (either local cluster, connect-only, or Ray Client)
        if ray_address and ray_address.startswith("ray://"):
            # Ray Client connection
            # Always pass runtime_env if available to keep module versions in sync
            init_kwargs = {
                "address": ray_address,
                "logging_level": "FATAL",
                "log_to_driver": False,
                "ignore_reinit_error": True,
            }
            if runtime_env:
                init_kwargs["runtime_env"] = runtime_env
            print(f"🌐 Connecting to Ray cluster: {ray_address}")
            if working_dir is not None:
                try:
                    print(f"📦 Uploading working dir: {os.path.abspath(working_dir)}")
                except Exception:
                    print("📦 Uploading working dir")
            try:
                ray.init(**init_kwargs)
                # print("🔗 Connected to Ray")
            except Exception as e:
                msg = str(e)
                # Tolerate repeated client init attempts
                if "already connected" in msg or "Ray Client is already connected" in msg:
                    print("ℹ️ Ray Client already connected; continuing")
                    return
                raise
        elif connect_only:
            # Connect to an existing Ray cluster without starting one
            # address="auto" will attempt to discover a running local cluster
            print("🔌 Connecting to existing Ray cluster (auto)")
            ray.init(
                address="auto",
                logging_level="FATAL",
                log_to_driver=False,
                include_dashboard=False,
                _temp_dir="/tmp/ray",
                configure_logging=False,
                ignore_reinit_error=True,
            )
            # print("🔗 Connected to Ray")
        else:
            # Local Ray cluster (may start one if none exists)
            # Include current working directory so remote workers can import local modules
            local_runtime_env = {"working_dir": os.getcwd()}
            # Add required modules for local cluster as well
            try:
                # Ship the entire project root so local workers import the same code
                import agent
                import terminal
                import utils
                local_runtime_env["py_modules"] = [agent, terminal, utils]
            except ImportError as e:
                print(f"Error importing modules: {e}")
                pass

            try:
                print(f"🧪 Starting local Ray cluster (cwd as working dir): {os.getcwd()}")
            except Exception:
                print("🧪 Starting local Ray cluster")
            ray.init(
                runtime_env=local_runtime_env,
                logging_level="FATAL",  # Only show fatal errors
                log_to_driver=False,  # Don't send raylet logs to driver
                include_dashboard=False,  # Disable dashboard to reduce log noise
                _temp_dir="/tmp/ray",  # Use consistent temp directory
                configure_logging=False,  # Don't configure Python logging
                ignore_reinit_error=True,  # Ignore reinitialization errors
            )
            print("✅ Local Ray ready")
    except Exception as e:
        raise RuntimeError(f"Failed to initialize Ray: {e}") from e


def get_node_resources(node_info: Dict) -> Dict:
    """Extract useful resource information from node info."""
    resources = node_info.get("Resources", {})
    return {
        "CPU": resources.get("CPU", 0),
        "memory": resources.get("memory", 0),
        "node_id": node_info.get("NodeID", ""),
        "node_ip": node_info.get("NodeManagerAddress", ""),
        "alive": node_info.get("Alive", False),
    }


def get_head_node_id() -> Optional[str]:
    """Return the head node's NodeID by resolving RAY_ADDRESS host to IP.

    Assumes Ray has already been initialized by the caller.
    """
    try:
        if not ray.is_initialized():
            raise RuntimeError(
                "Ray must be initialized before calling get_head_node_id()"
            )
        # Try to derive head IP from RAY_ADDRESS
        head_ip: Optional[str] = None
        addr = os.environ.get("RAY_ADDRESS")
        if addr:
            try:
                host_port = addr.split("://", 1)[1] if "://" in addr else addr
                host = host_port.split(":", 1)[0]
                # Resolve hostname to IP if necessary
                head_ip = socket.gethostbyname(host)
            except Exception:
                head_ip = None

        node_dicts = [n for n in ray.nodes() if n.get("Alive")]
        if not node_dicts:
            return None

        # Match by IP if we resolved one
        if head_ip:
            for d in node_dicts:
                node_ip = d.get("NodeManagerAddress") or d.get("node_ip")
                if node_ip == head_ip:
                    return d.get("NodeID") or d.get("node_id")

        # Fallbacks: flags or first alive node
        for d in node_dicts:
            if (
                d.get("is_head_node")
                or d.get("IsHead")
                or (str(d.get("node_type") or d.get("NodeType") or "").lower() == "head")
            ):
                return d.get("NodeID") or d.get("node_id")
        return node_dicts[0].get("NodeID") or node_dicts[0].get("node_id")
    except Exception as e:
        print(f"Failed to determine head node: {e}")
        raise RuntimeError(f"Failed to determine head node: {e}")


@lru_cache(maxsize=1)
def fetch_cluster_nodes() -> tuple[list[Dict], Optional[str]]:
    """Fetch Ray nodes (no State API) and return (normalized_nodes, head_node_id).

    - Assumes Ray has already been initialized by the caller.
    - Normalizes each node to keys: 'NodeID', 'NodeManagerAddress', 'Alive', 'Resources'.
    - Determines head_node_id from the same list by checking common flags.
    """
    if not ray.is_initialized():
        raise RuntimeError(
            "Ray must be initialized before calling fetch_cluster_nodes()"
        )

    try:
        raw_nodes = [n for n in ray.nodes() if n.get("Alive")]
    except Exception as e:
        raise RuntimeError(f"Failed to list nodes: {e}") from e

    if not raw_nodes:
        return [], None

    # Detect head, prefer matching RAY_ADDRESS host to IP
    head_node_id: Optional[str] = None
    head_ip: Optional[str] = None
    addr = os.environ.get("RAY_ADDRESS")
    if addr:
        try:
            host_port = addr.split("://", 1)[1] if "://" in addr else addr
            host = host_port.split(":", 1)[0]
            head_ip = socket.gethostbyname(host)
        except Exception:
            head_ip = None

    if head_ip:
        for d in raw_nodes:
            node_ip = d.get("NodeManagerAddress") or d.get("node_ip")
            if node_ip == head_ip:
                head_node_id = d.get("NodeID") or d.get("node_id")
                break

    # Fallback to flags
    for d in raw_nodes:
        if head_node_id:
            break
        if (
            d.get("is_head_node")
            or d.get("IsHead")
            or (str(d.get("node_type") or d.get("NodeType") or "").lower() == "head")
        ):
            head_node_id = d.get("NodeID") or d.get("node_id")
            break

    nodes: list[Dict] = []
    for d in raw_nodes:
        node_id = d.get("NodeID") or d.get("node_id")
        node_ip = d.get("NodeManagerAddress") or d.get("node_ip")
        resources = d.get("Resources") or d.get("resources_total") or {}

        nodes.append(
            {
                "NodeID": node_id,
                "NodeManagerAddress": node_ip,
                "NodeName": node_ip,
                "Alive": True,
                "Resources": resources,
            }
        )

    return nodes, head_node_id


# ============ Common helpers for service actors ============


def detect_accessible_ip() -> str:
    """Determine an accessible IP address without internet calls.

    Linux: use `ip route get` to extract src IP.
    macOS: use `route -n get default` -> `ipconfig getifaddr`.
    """
    try:
        system = platform.system()
        if system == "Darwin":
            cmd = (
                "IFACE=$(route -n get default 2>/dev/null | awk '/interface:/{print $2}') && "
                "ipconfig getifaddr $IFACE"
            )
        else:
            cmd = (
                "ip -o -4 route get 192.0.2.1 | "
                "awk '{for(i=1;i<=NF;i++) if($i==\"src\"){print $(i+1)}}'"
            )
        output = subprocess.check_output(
            ["bash", "-lc", cmd], stderr=subprocess.DEVNULL
        )
        ip = output.decode().strip().splitlines()[0].strip()
        if ip:
            return ip
    except Exception:
        pass
    return "127.0.0.1"


def adjust_port_for_macos(requested_port: int, fallback_port: int = 8888) -> int:
    """On macOS, avoid privileged port 80 during development."""
    try:
        if platform.system() == "Darwin" and int(requested_port) == 80:
            return int(fallback_port)
        return int(requested_port)
    except Exception:
        return requested_port


def quote_shell_single(path: str) -> str:
    """Safely single-quote a path for bash -lc string."""
    return path.replace("'", "'\"'\"'")


def sanitize_env_for_jupyter(env: dict) -> dict:
    """Remove env variables that would disable auth inadvertently."""
    new_env = dict(env)
    try:
        if new_env.get("JUPYTER_TOKEN", None) in ("", "''", '""'):
            new_env.pop("JUPYTER_TOKEN", None)
        if new_env.get("JUPYTER_PASSWORD", None) in ("", "''", '""'):
            new_env.pop("JUPYTER_PASSWORD", None)
    except Exception:
        pass
    return new_env


# ============ CLI/interactive helpers ============


def is_interactive_command(command: str) -> bool:
    """Heuristic to decide if a command needs an interactive session (stdin).

    Includes a curated set of known interactive programs and flag checks.
    """
    interactive_programs = {
        "python",
        "python3",
        "node",
        "nodejs",
        "ruby",
        "irb",
        "php",
        "mysql",
        "psql",
        "sqlite3",
        "redis-cli",
        "mongo",
        "bc",
        "ftp",
        "telnet",
        "ssh",
        "less",
        "more",
        "top",
        "htop",
        "vi",
        "nano",
        "emacs",
        "pico",
        "bash",
        "pip",
        "uv",
        "jupyter",
        "jupyter-lab",
        "code-server",
    }

    cmd_parts = command.strip().split()
    if not cmd_parts:
        return False
    base_cmd = cmd_parts[0].split("/")[-1]
    if base_cmd in interactive_programs:
        return True
    lowered = command.lower()
    if ("-i " in lowered) or ("--interactive" in lowered):
        return True
    return False


def filter_raylet_warnings(text: str) -> str:
    """Filter out noisy raylet file_system_monitor warnings from output text."""
    if not text:
        return text
    raylet_warning_pattern = r"\(raylet\) \[.*?\] \(raylet\) file_system_monitor\.cc.*?Object creation will fail if spilling is required\.\s*"
    return re.sub(raylet_warning_pattern, "", text, flags=re.MULTILINE | re.DOTALL)


def download_code_server_if_needed(os_name: str, arch: str) -> Optional[str]:
    """Download code-server for specified target platform if not cached.
    Args:
        os_name: 'linux' or 'macos' (code-server naming)
        arch: 'amd64' or 'arm64'
    Returns:
        Path to the downloaded archive, or None if download failed
    """
    import urllib.request
    import json
    import sys
    import re

    def _find_latest_cached(cache_dir: str) -> Optional[str]:
        """Find the newest cached archive for the given target (by semantic version)."""
        try:
            if not os.path.isdir(cache_dir):
                return None
            pattern = re.compile(
                rf"^code-server-(\d+\.\d+\.\d+)-{os_name}-{arch}\.tar\.gz$"
            )
            candidates: list[tuple[tuple[int, int, int], str]] = []
            for fname in os.listdir(cache_dir):
                m = pattern.match(fname)
                if not m:
                    continue
                ver_str = m.group(1)
                try:
                    parts = tuple(int(x) for x in ver_str.split("."))
                    if len(parts) == 3:
                        candidates.append((parts, os.path.join(cache_dir, fname)))
                except Exception:
                    continue
            if not candidates:
                return None
            candidates.sort(reverse=True)  # highest version first
            return candidates[0][1]
        except Exception:
            return None

    try:
        cache_dir = os.path.expanduser("~/.rayssh/client_cache")
        os.makedirs(cache_dir, exist_ok=True)

        # Try to get latest version from GitHub API
        version: Optional[str] = None
        print("🔍 Fetching latest code-server version for target...")
        api_url = "https://api.github.com/repos/coder/code-server/releases/latest"
        try:
            with urllib.request.urlopen(api_url, timeout=10) as response:
                release_data = json.loads(response.read().decode())
                tag = release_data.get("tag_name") or release_data.get("name")
                if tag:
                    version = str(tag).lstrip("v")
        except Exception as e:
            print(f"⚠️  Could not contact GitHub API: {e}")

        # If API failed, fall back to the newest cached archive (if any)
        if not version:
            cached_latest = _find_latest_cached(cache_dir)
            if cached_latest and os.path.exists(cached_latest):
                fname = os.path.basename(cached_latest)
                print(f"✅ Using newest cached code-server archive: {fname}")
                return cached_latest
            print(
                "❌ No cached code-server archive found for target and API is unreachable"
            )
            return None

        filename = f"code-server-{version}-{os_name}-{arch}.tar.gz"
        cached_file = os.path.join(cache_dir, filename)

        # Use cache if present
        if os.path.exists(cached_file):
            print(f"✅ Using cached code-server v{version} for {os_name}-{arch}")
            return cached_file

        # Download with progress
        download_url = f"https://github.com/coder/code-server/releases/download/v{version}/{filename}"
        print(f"⬇️  Downloading code-server v{version} for {os_name}-{arch}...")
        print(f"   URL: {download_url}")

        def _progress(count, block_size, total_size):
            downloaded = count * block_size
            if total_size > 0:
                pct = min(100, int(downloaded * 100 / total_size))
                total_mb = total_size / (1024 * 1024)
                done_mb = downloaded / (1024 * 1024)
                sys.stdout.write(
                    f"\r   Progress: {done_mb:.1f}/{total_mb:.1f} MB ({pct}%)"
                )
            else:
                done_mb = downloaded / (1024 * 1024)
                sys.stdout.write(f"\r   Progress: {done_mb:.1f} MB")
            sys.stdout.flush()

        temp_file = cached_file + ".tmp"
        try:
            urllib.request.urlretrieve(download_url, temp_file, _progress)
            sys.stdout.write("\n")
            os.rename(temp_file, cached_file)
            print(f"✅ Downloaded code-server to {cached_file}")
            return cached_file
        except Exception as e:
            # On download failure, fall back to newest cached if available
            print(f"⚠️  Download failed: {e}")
            try:
                if os.path.exists(temp_file):
                    os.remove(temp_file)
            except Exception:
                pass
            cached_latest = _find_latest_cached(cache_dir)
            if cached_latest and os.path.exists(cached_latest):
                fname = os.path.basename(cached_latest)
                print(f"✅ Falling back to newest cached archive: {fname}")
                return cached_latest
            print("❌ No cached archive available to fall back to")
            return None

    except Exception as e:
        print(f"❌ Error preparing code-server: {e}")
        return None
