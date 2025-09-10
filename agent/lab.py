import os
import subprocess
import threading
import time
from typing import Dict, Optional
from utils import (
    detect_accessible_ip,
    adjust_port_for_macos,
    quote_shell_single,
    sanitize_env_for_jupyter,
)

import ray
import shlex


@ray.remote(num_gpus=0)
class LabActor:
    """
    Ray actor that launches a Jupyter Lab server on the node where it is placed.
    """

    def __init__(self):
        self.cwd = os.getcwd()
        self.env = os.environ.copy()
        self.running_process: Optional[subprocess.Popen] = None
        self.process_lock = threading.Lock()
        self.log_file_path: Optional[str] = None
        self.host_ip: Optional[str] = None
        self.bound_port: Optional[int] = None
        self.root_dir: Optional[str] = None

    def start_lab(self, root_dir: Optional[str] = None, port: int = 80) -> Dict:
        """
        Start a Jupyter Lab server on the node.

        Args:
            root_dir: Root directory for Jupyter Lab. If None, uses current working directory.
                     If None and we're in a Ray Client session with uploaded working_dir,
                     we'll use the actual remote user's home directory instead.
            port: Port to bind. Should be 80 per requirement.

        Returns:
            Dict with keys: success, error (opt), log_file, pid, host_ip, root_dir, port
        """
        try:
            with self.process_lock:
                if self.running_process and self.running_process.poll() is None:
                    return {
                        "success": False,
                        "error": "Jupyter Lab already running",
                        "log_file": self.log_file_path,
                        "pid": self.running_process.pid
                        if self.running_process
                        else None,
                        "host_ip": self.host_ip,
                        "root_dir": root_dir or self.cwd,
                        "port": port,
                    }

                # Determine launch root directory
                if root_dir is None:
                    # Check if we're in a Ray Client uploaded working directory
                    # If so, use the remote user's home directory instead
                    if "_ray_pkg_" in self.cwd or "working_dir_files" in self.cwd:
                        launch_root = os.path.expanduser("~")
                    else:
                        launch_root = self.cwd
                else:
                    launch_root = root_dir

                launch_root = os.path.expanduser(launch_root)

                if not os.path.isabs(launch_root):
                    # For relative paths, use appropriate base directory
                    if root_dir is None:
                        # No path specified: use home dir if in uploaded workspace, else use cwd
                        base_dir = (
                            os.path.expanduser("~")
                            if "_ray_pkg_" in self.cwd
                            else self.cwd
                        )
                    else:
                        # Path was specified: always use the current working directory as base
                        # (which could be uploaded working dir if user specified a path)
                        base_dir = self.cwd
                    launch_root = os.path.abspath(os.path.join(base_dir, launch_root))

                if not os.path.isdir(launch_root):
                    return {
                        "success": False,
                        "error": f"Root dir not found: {launch_root}",
                    }

                os.makedirs(os.path.expanduser("~/.rayssh"), exist_ok=True)
                timestamp = int(time.time())
                self.log_file_path = os.path.expanduser(
                    f"~/.rayssh/jupyter-lab-{timestamp}.log"
                )

                self.host_ip = detect_accessible_ip()

                # On macOS development machines, avoid privileged ports
                port = adjust_port_for_macos(port, 8888)

                # Persist chosen values
                self.bound_port = int(port)
                self.root_dir = launch_root

                # Ensure jupyterlab is installed in the current environment if missing
                try:
                    __import__(
                        "jupyterlab"
                    )  # Lazy check without hard import dependency
                except Exception:
                    try:
                        # Attempt fast install with uv if available
                        print("🛠️ Installing jupyterlab...")
                        subprocess.check_call(
                            [
                                "bash",
                                "-lc",
                                "command -v uv >/dev/null 2>&1 && uv pip install jupyterlab || pip install --user jupyterlab",
                            ],
                            stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL,
                        )
                    except Exception:
                        # Fallback to pip
                        subprocess.check_call(
                            ["bash", "-lc", "pip install --user jupyterlab"],
                            stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL,
                        )

                # Quote root dir for the shell
                quoted_root = quote_shell_single(launch_root)

                # Create a temporary Jupyter config to ensure allow_root is set
                temp_config_dir = os.path.expanduser("~/.rayssh")
                os.makedirs(temp_config_dir, exist_ok=True)
                jupyter_config_file = os.path.join(
                    temp_config_dir, f"jupyter_config_{timestamp}.py"
                )
                with open(jupyter_config_file, "w") as f:
                    f.write("c.ServerApp.allow_root = True\n")
                    f.write(f"c.ServerApp.root_dir = '{launch_root}'\n")
                    f.write(f"c.ServerApp.ip = '{self.host_ip}'\n")
                    f.write(f"c.ServerApp.port = {int(port)}\n")
                    f.write("c.ServerApp.open_browser = False\n")
                    f.write("c.ServerApp.allow_origin = '*'\n")

                # Use the config file instead of command line arguments
                jupyter_cmd = [
                    "jupyter",
                    "lab",
                    "--allow-root",
                    f"--config={jupyter_config_file}",
                ]

                # Use bash -lc with proper argument separation
                cmd = ["bash", "-lc", shlex.join(jupyter_cmd)]

                print(f"🚀 Starting Jupyter Lab with command: {cmd}")
                print(f"🚀 Jupyter config file: {jupyter_config_file}")

                # Prepare environment: avoid env that disables auth
                launch_env = sanitize_env_for_jupyter(self.env)

                # Start process and redirect output to the log file
                with open(self.log_file_path, "ab", buffering=0) as logf:
                    self.running_process = subprocess.Popen(
                        cmd,
                        cwd=launch_root,
                        env=launch_env,
                        stdout=logf,
                        stderr=subprocess.STDOUT,
                    )

                return {
                    "success": True,
                    "log_file": self.log_file_path,
                    "pid": self.running_process.pid,
                    "host_ip": self.host_ip,
                    "root_dir": self.root_dir,
                    "cwd": self.cwd,
                    "port": self.bound_port,
                    "url": f"http://{self.host_ip}:{self.bound_port}/lab",
                    "cmd": cmd,
                }
        except Exception as e:
            return {"success": False, "error": str(e)}

    def get_state(self) -> Dict:
        """Return current state of the Lab process and metadata."""
        pid = None
        running = False
        if self.running_process is not None:
            pid = self.running_process.pid
            running = self.running_process.poll() is None
        return {
            "running": running,
            "pid": pid,
            "log_file": self.log_file_path,
            "host_ip": self.host_ip,
            "cwd": self.cwd,
            "port": self.bound_port,
            "root_dir": self.root_dir,
        }

    def get_info(self) -> Dict:
        """Return a consolidated info dict including a URL if determinable."""
        st = self.get_state()
        host_ip = st.get("host_ip")
        port = st.get("port")
        url = None
        if host_ip and port:
            url = f"http://{host_ip}:{port}/lab"
        st["url"] = url
        return st

    def get_log_path(self) -> Optional[str]:
        return self.log_file_path

    def read_log_chunk(self, offset: int = 0, max_bytes: int = 65536) -> Dict:
        """
        Read a chunk from the log file starting at the given byte offset.
        Returns dict with 'data', 'next_offset', 'eof'.
        """
        path = self.log_file_path
        if not path or not os.path.exists(path):
            return {"data": "", "next_offset": offset, "eof": True}
        try:
            with open(path, "rb") as f:
                f.seek(offset)
                data = f.read(max_bytes)
                next_off = f.tell()
            return {
                "data": data.decode(errors="ignore"),
                "next_offset": next_off,
                "eof": False,
            }
        except Exception:
            return {"data": "", "next_offset": offset, "eof": True}

    def stop(self) -> bool:
        """Attempt to terminate the lab process."""
        with self.process_lock:
            if self.running_process is None:
                return True
            try:
                self.running_process.terminate()
                try:
                    self.running_process.wait(timeout=10)
                except Exception:
                    self.running_process.kill()
                return True
            finally:
                self.running_process = None
