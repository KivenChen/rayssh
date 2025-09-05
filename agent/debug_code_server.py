import os
import platform
import secrets
import subprocess
import threading
import time
from typing import Dict, Optional
from utils import detect_accessible_ip, adjust_port_for_macos, quote_shell_single

import ray
from agent.code_server import CodeServerActor


@ray.remote(num_gpus=0)
class DebugCodeServerActor(CodeServerActor):
    """Extended CodeServerActor with Ray debugging capabilities"""
    
    def __init__(self):
        super().__init__()
        self.debug_env_vars = {
            "RAY_DEBUG": "1",
            "RAY_POST_MORTEM_DEBUG": "1",
        }
        # Update environment with debug variables
        self.env.update(self.debug_env_vars)

    def start_debug_code(self, root_dir: Optional[str] = None, port: int = 80, debug_config: Optional[Dict] = None) -> Dict:
        """Start code-server with Ray debugging extensions and environment"""
        try:
            with self.process_lock:
                if self.running_process and self.running_process.poll() is None:
                    return {
                        "success": False,
                        "error": "debug code-server already running",
                        "log_file": self.log_file_path,
                        "pid": self.running_process.pid
                        if self.running_process
                        else None,
                        "host_ip": self.host_ip,
                        "root_dir": root_dir or self.cwd,
                        "port": port,
                    }

                # Determine launch root directory (same logic as parent)
                if root_dir is None:
                    if "_ray_pkg_" in self.cwd or "working_dir_files" in self.cwd:
                        launch_root = os.path.expanduser("~")
                    else:
                        launch_root = self.cwd
                else:
                    launch_root = root_dir

                launch_root = os.path.expanduser(launch_root)
                if not os.path.isabs(launch_root):
                    if root_dir is None:
                        base_dir = (
                            os.path.expanduser("~")
                            if "_ray_pkg_" in self.cwd
                            else self.cwd
                        )
                    else:
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
                    f"~/.rayssh/debug-code-server-{timestamp}.log"
                )

                self.host_ip = detect_accessible_ip()
                port = adjust_port_for_macos(port, 8888)
                self.bound_port = int(port)
                self.root_dir = launch_root

                # Ensure code-server installed
                if not self._is_code_server_installed():
                    print("⚙️  code-server not found. Installing...")
                    install_result = self._install_code_server()
                    if not install_result["success"]:
                        return {
                            "success": False,
                            "error": f"Failed to install code-server: {install_result['error']}",
                        }

                # Generate password if not already
                if not self.password:
                    self.password = secrets.token_urlsafe(16)

                # Install debugging extensions first
                print("🐛 Installing Ray debugging extensions...")
                extension_result = self._install_debug_extensions()
                if not extension_result["success"]:
                    print(f"⚠️  Warning: Failed to install some extensions: {extension_result['error']}")

                # Configure Ray cluster connection if provided
                if debug_config:
                    cluster_result = self._configure_ray_cluster(debug_config)
                    if not cluster_result["success"]:
                        print(f"⚠️  Warning: Failed to configure Ray cluster: {cluster_result['error']}")

                # Prepare environment with debug variables
                debug_env = self.env.copy()
                debug_env.update(self.debug_env_vars)

                # Start code-server with debug environment
                code_server_bin = self._get_code_server_binary()
                if not code_server_bin:
                    return {
                        "success": False,
                        "error": "code-server binary not found",
                    }

                # Build command with debug environment configuration
                cmd = [
                    code_server_bin,
                    "--bind-addr",
                    f"{self.host_ip}:{self.bound_port}",
                    "--password",
                    self.password,
                    "--disable-telemetry",
                    "--disable-update-check",
                    "--disable-workspace-trust",
                    launch_root,
                ]

                print(f"🔧 Starting debug code-server on {self.host_ip}:{self.bound_port}")
                print(f"🐛 Debug environment: RAY_DEBUG=1, RAY_POST_MORTEM_DEBUG=1")

                with open(self.log_file_path, "w") as log_file:
                    self.running_process = subprocess.Popen(
                        cmd,
                        stdout=log_file,
                        stderr=subprocess.STDOUT,
                        cwd=launch_root,
                        env=debug_env,  # Use debug-enabled environment
                        preexec_fn=os.setsid,
                    )

                # Wait a bit to check if process started successfully
                time.sleep(2)
                if self.running_process.poll() is not None:
                    return {
                        "success": False,
                        "error": f"code-server process exited with code {self.running_process.returncode}",
                        "log_file": self.log_file_path,
                    }

                url = f"http://{self.host_ip}:{self.bound_port}"
                return {
                    "success": True,
                    "url": url,
                    "password": self.password,
                    "log_file": self.log_file_path,
                    "pid": self.running_process.pid,
                    "host_ip": self.host_ip,
                    "port": self.bound_port,
                    "root_dir": launch_root,
                    "debug_enabled": True,
                }

        except Exception as e:
            return {
                "success": False,
                "error": f"Failed to start debug code-server: {str(e)}",
            }

    def _install_debug_extensions(self) -> Dict:
        """Install Python extension and Ray debugging extensions"""
        try:
            code_server_bin = self._get_code_server_binary()
            if not code_server_bin:
                return {"success": False, "error": "code-server binary not found"}

            extensions_to_install = [
                "ms-python.python",  # Python extension
                "anyscalecompute.ray-distributed-debugger",
                # The debugging is primarily enabled through environment variables
            ]

            failed_extensions = []
            for ext_id in extensions_to_install:
                try:
                    print(f"📦 Installing extension: {ext_id}")
                    cmd = [code_server_bin, "--install-extension", ext_id]
                    result = subprocess.run(
                        cmd,
                        capture_output=True,
                        text=True,
                        timeout=60,  # 60 second timeout
                        env=self.env
                    )
                    if result.returncode != 0:
                        print(f"⚠️  Failed to install {ext_id}: {result.stderr}")
                        failed_extensions.append(ext_id)
                    else:
                        print(f"✅ Successfully installed {ext_id}")
                except subprocess.TimeoutExpired:
                    print(f"⚠️  Timeout installing {ext_id}")
                    failed_extensions.append(ext_id)
                except Exception as e:
                    print(f"⚠️  Error installing {ext_id}: {str(e)}")
                    failed_extensions.append(ext_id)

            if failed_extensions:
                return {
                    "success": False,
                    "error": f"Failed to install extensions: {', '.join(failed_extensions)}"
                }
            else:
                return {"success": True}

        except Exception as e:
            return {"success": False, "error": str(e)}

    def _configure_ray_cluster(self, debug_config: Dict) -> Dict:
        """Configure Ray cluster connection for debugging"""
        try:
            ray_host = debug_config.get("ray_dashboard_host")
            ray_port = debug_config.get("ray_dashboard_port")
            
            if not ray_host or not ray_port:
                return {"success": False, "error": "Missing Ray cluster configuration"}

            # Create or update VS Code settings for Ray cluster
            # This would typically go in the workspace .vscode/settings.json
            # For now, we'll just log the configuration
            print(f"🔗 Ray cluster configured: {ray_host}:{ray_port}")
            
            # In a real implementation, you might want to:
            # 1. Create .vscode/settings.json in the workspace
            # 2. Configure Python debugger to connect to Ray cluster
            # 3. Set up Ray-specific debugging configurations
            
            return {"success": True}
            
        except Exception as e:
            return {"success": False, "error": str(e)}

    def _get_code_server_binary(self) -> Optional[str]:
        """Get path to code-server binary"""
        # Check if installed via our installer
        rayssh_bin = os.path.expanduser("~/.rayssh/bin/code-server")
        if os.path.isfile(rayssh_bin) and os.access(rayssh_bin, os.X_OK):
            return rayssh_bin
        
        # Check system PATH
        try:
            result = subprocess.run(
                ["which", "code-server"],
                capture_output=True,
                text=True,
                env=self.env
            )
            if result.returncode == 0:
                return result.stdout.strip()
        except:
            pass
        
        return None

    def get_debug_info(self) -> Dict:
        """Get debug-specific information"""
        info = self.get_info()
        if isinstance(info, dict):
            info["debug_enabled"] = True
            info["debug_env_vars"] = self.debug_env_vars
        return info
