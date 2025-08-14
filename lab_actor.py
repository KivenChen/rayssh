import os
import platform
import subprocess
import threading
import time
from typing import Dict, Optional

import ray


def _detect_accessible_ip() -> str:
    """
    Determine an accessible IP address using the prescribed method:
    ip -o -4 route get 192.0.2.1 | awk '{for(i=1;i<=NF;i++) if($i=="src"){print $(i+1)}}'
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
        output = subprocess.check_output(["bash", "-lc", cmd], stderr=subprocess.DEVNULL)
        ip = output.decode().strip().splitlines()[0].strip()
        if ip:
            return ip
    except Exception:
        pass

    # Fallback without attempting internet connectivity
    return "127.0.0.1"


@ray.remote
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
                        "pid": self.running_process.pid if self.running_process else None,
                        "host_ip": self.host_ip,
                        "root_dir": root_dir or self.cwd,
                        "port": port,
                    }

                launch_root = root_dir or self.cwd
                launch_root = os.path.expanduser(launch_root)
                if not os.path.isabs(launch_root):
                    launch_root = os.path.abspath(os.path.join(self.cwd, launch_root))
                if not os.path.isdir(launch_root):
                    return {"success": False, "error": f"Root dir not found: {launch_root}"}

                os.makedirs(os.path.expanduser("~/.rayssh"), exist_ok=True)
                timestamp = int(time.time())
                self.log_file_path = os.path.expanduser(f"~/.rayssh/jupyter-lab-{timestamp}.log")

                self.host_ip = _detect_accessible_ip()

                # On macOS development machines, avoid privileged ports
                try:
                    if platform.system() == "Darwin" and int(port) == 80:
                        port = 8888
                except Exception:
                    pass

                # Persist chosen values
                self.bound_port = int(port)
                self.root_dir = launch_root

                # Ensure jupyterlab is installed in the current environment if missing
                try:
                    __import__("jupyterlab")  # Lazy check without hard import dependency
                except Exception:
                    try:
                        # Attempt fast install with uv if available
                        subprocess.check_call(["bash", "-lc", "command -v uv >/dev/null 2>&1 && uv pip install jupyterlab || pip install --user jupyterlab"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    except Exception:
                        # Fallback to pip
                        subprocess.check_call(["bash", "-lc", "pip install --user jupyterlab"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

                # Quote root dir for the shell
                quoted_root = launch_root.replace("'", "'\"'\"'")
                cmd = [
                    "bash",
                    "-lc",
                    (
                        f"jupyter lab "
                        f"--ServerApp.root_dir='{quoted_root}' "
                        f"--ServerApp.ip={self.host_ip} "
                        f"--ServerApp.port={int(port)} "
                        f"--ServerApp.open_browser=False "
                        f"--ServerApp.allow_origin='*'"
                    ),
                ]

                # Prepare environment: avoid env that disables auth
                launch_env = dict(self.env)
                try:
                    if launch_env.get("JUPYTER_TOKEN", None) in ("", "''", '""'):
                        launch_env.pop("JUPYTER_TOKEN", None)
                    if launch_env.get("JUPYTER_PASSWORD", None) in ("", "''", '""'):
                        launch_env.pop("JUPYTER_PASSWORD", None)
                except Exception:
                    pass

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
                    "port": self.bound_port,
                    "url": f"http://{self.host_ip}:{self.bound_port}/lab",
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
            return {"data": data.decode(errors="ignore"), "next_offset": next_off, "eof": False}
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

