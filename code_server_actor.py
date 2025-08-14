import os
import platform
import secrets
import subprocess
import threading
import time
from typing import Dict, Optional
from utils import detect_accessible_ip, adjust_port_for_macos, quote_shell_single

import ray


def _detect_accessible_ip() -> str:
    return detect_accessible_ip()


@ray.remote
class CodeServerActor:
    def __init__(self):
        self.cwd = os.getcwd()
        self.env = os.environ.copy()
        self.running_process: Optional[subprocess.Popen] = None
        self.process_lock = threading.Lock()
        self.log_file_path: Optional[str] = None
        self.host_ip: Optional[str] = None
        self.bound_port: Optional[int] = None
        self.root_dir: Optional[str] = None
        self.password: Optional[str] = None

    def start_code(self, root_dir: Optional[str] = None, port: int = 80) -> Dict:
        try:
            with self.process_lock:
                if self.running_process and self.running_process.poll() is None:
                    return {
                        "success": False,
                        "error": "code-server already running",
                        "log_file": self.log_file_path,
                        "pid": self.running_process.pid
                        if self.running_process
                        else None,
                        "host_ip": self.host_ip,
                        "root_dir": root_dir or self.cwd,
                        "port": port,
                    }

                launch_root = root_dir or self.cwd
                launch_root = os.path.expanduser(launch_root)
                if not os.path.isabs(launch_root):
                    launch_root = os.path.abspath(os.path.join(self.cwd, launch_root))
                if not os.path.isdir(launch_root):
                    return {
                        "success": False,
                        "error": f"Root dir not found: {launch_root}",
                    }

                os.makedirs(os.path.expanduser("~/.rayssh"), exist_ok=True)
                timestamp = int(time.time())
                self.log_file_path = os.path.expanduser(
                    f"~/.rayssh/code-server-{timestamp}.log"
                )

                self.host_ip = _detect_accessible_ip()

                port = adjust_port_for_macos(port, 8888)

                self.bound_port = int(port)
                self.root_dir = launch_root

                # Ensure code-server installed
                install_cmd = (
                    "if ! command -v code-server >/dev/null 2>&1; then "
                    "  if [ -x ~/.rayssh/install_code_server.sh ]; then ~/.rayssh/install_code_server.sh; "
                    "  else curl -fsSL https://code-server.dev/install.sh | sh; fi; "
                    "fi"
                )
                try:
                    subprocess.check_call(
                        ["bash", "-lc", install_cmd],
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                    )
                except Exception:
                    return {
                        "success": False,
                        "error": "Failed to install code-server automatically",
                    }

                # Generate password if not already
                if not self.password:
                    self.password = secrets.token_urlsafe(16)

                # Quote root
                quoted_root = quote_shell_single(launch_root)
                cmd = [
                    "bash",
                    "-lc",
                    (
                        f"code-server '{quoted_root}' "
                        f"--bind-addr {self.host_ip}:{self.bound_port} "
                        f"--auth password "
                        f"--disable-telemetry "
                        f"--disable-update-check"
                    ),
                ]

                launch_env = dict(self.env)
                launch_env["PASSWORD"] = self.password

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
                    "url": f"http://{self.host_ip}:{self.bound_port}/",
                    "password": self.password,
                }
        except Exception as e:
            return {"success": False, "error": str(e)}

    def get_state(self) -> Dict:
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
            "password": self.password,
        }

    def get_info(self) -> Dict:
        st = self.get_state()
        host_ip = st.get("host_ip")
        port = st.get("port")
        url = None
        if host_ip and port:
            url = f"http://{host_ip}:{port}/"
        st["url"] = url
        return st

    def get_log_path(self) -> Optional[str]:
        return self.log_file_path

    def has_code_server(self) -> bool:
        try:
            subprocess.check_call(
                ["bash", "-lc", "command -v code-server >/dev/null 2>&1"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            return True
        except Exception:
            return False

    def read_log_chunk(self, offset: int = 0, max_bytes: int = 65536) -> Dict:
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
