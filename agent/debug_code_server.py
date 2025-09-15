import os
import platform
import secrets
import subprocess
import threading
import time
from typing import Dict, Optional
from utils import detect_accessible_ip, adjust_port_for_macos, quote_shell_single

import ray


@ray.remote(num_gpus=0)
class DebugCodeServerActor:
    """Extended CodeServerActor with Ray debugging capabilities"""

    def __init__(self):
        # Local state (do NOT inherit from another Ray actor class)
        self.cwd = os.getcwd()
        self.env = os.environ.copy()
        self.running_process: Optional[subprocess.Popen] = None
        self.process_lock = threading.Lock()
        self.log_file_path: Optional[str] = None
        self.host_ip: Optional[str] = None
        self.bound_port: Optional[int] = None
        self.root_dir: Optional[str] = None
        self.password: Optional[str] = None

        # Debug environment flags for Ray
        self.debug_env_vars = {
            "RAY_DEBUG": "1",
            "RAY_POST_MORTEM_DEBUG": "1",
        }
        # Update environment with debug variables
        self.env.update(self.debug_env_vars)

    def start_debug_code(
        self,
        root_dir: Optional[str] = None,
        port: int = 80,
        debug_config: Optional[Dict] = None,
    ) -> Dict:
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
                    print(
                        f"⚠️  Warning: Failed to install some extensions: {extension_result['error']}"
                    )

                # Configure Ray cluster connection if provided
                if debug_config:
                    try:
                        self.debug_config = dict(debug_config)
                    except Exception:
                        self.debug_config = {"raw": str(debug_config)}
                    cluster_result = self._configure_ray_cluster(debug_config)
                    if not cluster_result.get("success"):
                        print(
                            f"⚠️  Warning: Failed to configure Ray cluster: {cluster_result.get('error')}"
                        )

                # Prepare environment with debug variables and password
                launch_env = dict(self.env)
                launch_env.update(self.debug_env_vars)
                if self.password:
                    launch_env["PASSWORD"] = self.password

                # Ensure we can find code-server (check multiple locations) and launch via bash -lc
                # Quote root path safely
                quoted_root = quote_shell_single(launch_root)
                code_server_cmd = (
                    f'export PATH="$HOME/.rayssh/bin:$HOME/.local/bin:$PATH" && '
                    f"code-server '{quoted_root}' "
                    f"--bind-addr {self.host_ip}:{self.bound_port} "
                    f"--auth password "
                    f"--disable-telemetry "
                    f"--disable-update-check"
                )

                cmd = [
                    "bash",
                    "-lc",
                    code_server_cmd,
                ]

                print(
                    f"🔧 Starting debug code-server on {self.host_ip}:{self.bound_port}"
                )
                print(f"🐛 Debug environment: RAY_DEBUG=1, RAY_POST_MORTEM_DEBUG=1")

                with open(self.log_file_path, "ab", buffering=0) as log_file:
                    self.running_process = subprocess.Popen(
                        cmd,
                        stdout=log_file,
                        stderr=subprocess.STDOUT,
                        cwd=launch_root,
                        env=launch_env,
                    )

                # Wait a bit to check if process started successfully
                time.sleep(2)
                if self.running_process.poll() is not None:
                    return {
                        "success": False,
                        "error": f"code-server process exited with code {self.running_process.returncode}",
                        "log_file": self.log_file_path,
                        "attempted_cmd": code_server_cmd,
                        "host_ip": self.host_ip,
                        "port": self.bound_port,
                        "root_dir": launch_root,
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
                        env=self.env,
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
                    "error": f"Failed to install extensions: {', '.join(failed_extensions)}",
                }
            else:
                return {"success": True}

        except Exception as e:
            return {"success": False, "error": str(e)}

    # TODO(kiv): examine the real effective settings
    def _configure_ray_cluster(self, debug_config: Dict) -> Dict:
        """Configure Ray cluster connection for debugging"""
        try:
            # Prefer the full dashboard URL if available
            dashboard_url = debug_config.get("ray_dashboard_url")
            ray_host = debug_config.get("ray_dashboard_host")
            ray_port = debug_config.get("ray_dashboard_port")

            if dashboard_url:
                print(f"🔗 Ray cluster configured: {dashboard_url}")
            elif ray_host and ray_port:
                print(f"🔗 Ray cluster configured: {ray_host}:{ray_port}")
            else:
                return {"success": False, "error": "Missing Ray cluster configuration"}

            # Set environment variables for Ray debugging
            if dashboard_url:
                # Extract host and port from full URL for environment variables
                if dashboard_url.startswith("http://"):
                    url_part = dashboard_url[7:]  # Remove 'http://'
                    if ":" in url_part:
                        host, port_str = url_part.split(":", 1)
                        self.env["RAY_DASHBOARD_HOST"] = host
                        self.env["RAY_DASHBOARD_PORT"] = port_str
                    else:
                        self.env["RAY_DASHBOARD_HOST"] = url_part
                        self.env["RAY_DASHBOARD_PORT"] = "8265"
                else:
                    self.env["RAY_DASHBOARD_HOST"] = ray_host or "localhost"
                    self.env["RAY_DASHBOARD_PORT"] = str(ray_port or 8265)
            else:
                self.env["RAY_DASHBOARD_HOST"] = ray_host
                self.env["RAY_DASHBOARD_PORT"] = str(ray_port)

            # Also set RAY_ADDRESS if not already set, inferring client port from dashboard port
            try:
                host = self.env.get("RAY_DASHBOARD_HOST") or "localhost"
                dash_port = int(self.env.get("RAY_DASHBOARD_PORT") or 8265)
                # Common mapping: client 10001 <-> dashboard 8265; otherwise, inverse +/- 1736
                client_port = 10001 if dash_port == 8265 else (dash_port + 1736)
                if not self.env.get("RAY_ADDRESS"):
                    self.env["RAY_ADDRESS"] = f"ray://{host}:{client_port}"
            except Exception:
                pass

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

        # Check common local install location
        local_bin = os.path.expanduser("~/.local/bin/code-server")
        if os.path.isfile(local_bin) and os.access(local_bin, os.X_OK):
            return local_bin

        # Check system PATH
        try:
            result = subprocess.run(
                ["which", "code-server"], capture_output=True, text=True, env=self.env
            )
            if result.returncode == 0:
                return result.stdout.strip()
        except:
            pass

        return None

    def has_code_server(self) -> bool:
        """Check if code-server is available."""
        return self._is_code_server_installed()

    # ----- Helpers mirrored from CodeServerActor (no inheritance) -----

    def _is_code_server_installed(self) -> bool:
        """Check if code-server is installed and available in PATH."""
        try:
            # First check ~/.rayssh/bin (our custom install location)
            rayssh_binary = os.path.expanduser("~/.rayssh/bin/code-server")
            if os.path.exists(rayssh_binary):
                return True

            # Then check system PATH
            result = subprocess.run(
                ["bash", "-lc", "command -v code-server"],
                capture_output=True,
                text=True,
            )
            if result.returncode == 0:
                return True

            # Then check ~/.local/bin specifically
            result = subprocess.run(
                ["bash", "-lc", "command -v ~/.local/bin/code-server"],
                capture_output=True,
                text=True,
            )
            return result.returncode == 0
        except Exception:
            return False

    def _install_code_server(self) -> Dict:
        """Install code-server from shipped binary or fallback to download."""
        try:
            shipped_archive = self._find_shipped_code_server()
            if shipped_archive:
                print(f"📦 Found shipped code-server archive: {shipped_archive}")
                return self._install_from_archive(shipped_archive)
            else:
                print("📡 No shipped archive found, attempting download...")
                return self._install_from_download()
        except Exception as e:
            return {"success": False, "error": f"Installation error: {str(e)}"}

    def _find_shipped_code_server(self) -> Optional[str]:
        """Find shipped code-server archive in current working directory."""
        try:
            for item in os.listdir(self.cwd):
                if item.startswith("code-server-") and item.endswith(".tar.gz"):
                    archive_path = os.path.join(self.cwd, item)
                    if os.path.isfile(archive_path):
                        return archive_path
            return None
        except Exception:
            return None

    def _install_from_archive(self, archive_path: str) -> Dict:
        """Install code-server from a local archive file."""
        try:
            install_prefix = os.path.expanduser("~/.rayssh")
            bin_dir = os.path.join(install_prefix, "bin")
            lib_dir = os.path.join(install_prefix, "lib")

            print(f"📁 Creating installation directories...")
            os.makedirs(bin_dir, exist_ok=True)
            os.makedirs(lib_dir, exist_ok=True)

            filename = os.path.basename(archive_path)
            parts = filename.replace(".tar.gz", "").split("-")
            if len(parts) >= 3:
                version = parts[2]
            else:
                version = "unknown"

            print(f"📦 Extracting code-server archive...")
            import tarfile

            with tarfile.open(archive_path, "r:gz") as tar:
                tar.extractall(lib_dir)

            extracted_dirs = [
                d for d in os.listdir(lib_dir) if d.startswith("code-server-")
            ]
            if not extracted_dirs:
                return {
                    "success": False,
                    "error": "No extracted code-server directory found",
                }

            extracted_dir = extracted_dirs[0]
            full_extracted_path = os.path.join(lib_dir, extracted_dir)

            target_dir = os.path.join(lib_dir, f"code-server-{version}")
            if full_extracted_path != target_dir:
                if os.path.exists(target_dir):
                    import shutil

                    shutil.rmtree(target_dir)
                os.rename(full_extracted_path, target_dir)

            binary_source = os.path.join(target_dir, "bin", "code-server")
            binary_target = os.path.join(bin_dir, "code-server")

            if not os.path.exists(binary_source):
                return {
                    "success": False,
                    "error": f"code-server binary not found in {binary_source}",
                }

            if os.path.exists(binary_target) or os.path.islink(binary_target):
                os.remove(binary_target)

            print(f"🔗 Creating symlink...")
            os.symlink(binary_source, binary_target)

            os.chmod(binary_source, 0o755)

            print(f"✅ code-server installed successfully!")
            print(f"📍 Location: {target_dir}")
            print(f"🔗 Binary: {binary_target}")

            if os.path.exists(binary_target):
                return {"success": True}
            else:
                return {
                    "success": False,
                    "error": "Installation completed but binary not accessible",
                }
        except Exception as e:
            return {"success": False, "error": f"Archive installation failed: {str(e)}"}

    def _install_from_download(self) -> Dict:
        """Fallback: Install code-server by downloading (original method)."""
        try:
            script_path = os.path.expanduser("~/.rayssh/install_code_server.sh")
            os.makedirs(os.path.dirname(script_path), exist_ok=True)

            script_content = self._get_install_script_content()
            with open(script_path, "w") as f:
                f.write(script_content)
            os.chmod(script_path, 0o755)

            install_log = os.path.expanduser("~/.rayssh/code-server-install.log")

            print(f"🔧 Installing code-server via download (log: {install_log})")

            with open(install_log, "w") as log_file:
                process = subprocess.Popen(
                    ["bash", script_path, "--method", "standalone"],
                    stdout=log_file,
                    stderr=subprocess.STDOUT,
                    text=True,
                )

                process.wait()

                if process.returncode == 0:
                    print("✅ code-server installation completed successfully")
                    return {"success": True}
                else:
                    print(
                        f"❌ code-server installation failed (exit code: {process.returncode})"
                    )
                    try:
                        with open(install_log, "r") as f:
                            lines = f.readlines()
                            error_context = (
                                "".join(lines[-10:]) if lines else "No log output"
                            )
                    except Exception:
                        error_context = "Could not read installation log"

                    return {
                        "success": False,
                        "error": f"Installation failed with exit code {process.returncode}. Check {install_log} for details. Last output: {error_context[:500]}",
                    }
        except Exception as e:
            return {"success": False, "error": f"Download installation error: {str(e)}"}

    def _get_install_script_content(self) -> str:
        """Return the embedded installation script content."""
        return """#!/bin/bash
set -euo pipefail

echo "🔧 Starting code-server installation..."

# Function to detect OS and architecture  
detect_platform() {
    OS=$(uname -s | tr '[:upper:]' '[:lower:]')
    ARCH=$(uname -m)
    
    case $ARCH in
        x86_64) ARCH="amd64" ;;
        aarch64) ARCH="arm64" ;;
        *) echo "❌ Unsupported architecture: $ARCH" >&2; exit 1 ;;
    esac
    
    case $OS in
        linux) ;;
        darwin) OS="macos" ;;
        *) echo "❌ Unsupported OS: $OS" >&2; exit 1 ;;
    esac
}

# Function to check download tool availability
check_download_tool() {
    if command -v wget >/dev/null 2>&1; then
        DOWNLOAD_TOOL="wget"
        echo "📡 Using wget for downloads"
    elif command -v curl >/dev/null 2>&1; then
        DOWNLOAD_TOOL="curl"
        echo "📡 Using curl for downloads"
    else
        echo "❌ Neither wget nor curl is available" >&2
        exit 1
    fi
}

# Function to download with fallback
download_file() {
    local url="$1"
    local output="$2"
    
    if [ "$DOWNLOAD_TOOL" = "wget" ]; then
        wget --progress=bar:force -O "$output" "$url"
    else
        curl -fL --progress-bar -o "$output" "$url"
    fi
}

# Function to get latest version
get_latest_version() {
    echo "🔍 Fetching latest code-server version..."
    
    if [ "$DOWNLOAD_TOOL" = "wget" ]; then
        VERSION=$(wget -qO- https://api.github.com/repos/coder/code-server/releases/latest | grep '"tag_name"' | cut -d'"' -f4 | sed 's/^v//')
    else
        VERSION=$(curl -fsSL https://api.github.com/repos/coder/code-server/releases/latest | grep '"tag_name"' | cut -d'"' -f4 | sed 's/^v//')
    fi
    
    if [ -z "$VERSION" ]; then
        echo "❌ Failed to get latest version" >&2
        exit 1
    fi
    echo "📦 Latest version: $VERSION"
}

# Function to install standalone
install_standalone() {
    INSTALL_PREFIX="$HOME/.local"
    CACHE_DIR="$HOME/.cache/code-server"
    
    echo "📁 Creating directories..."
    mkdir -p "$CACHE_DIR" "$INSTALL_PREFIX/bin" "$INSTALL_PREFIX/lib"
    
    DOWNLOAD_URL="https://github.com/coder/code-server/releases/download/v$VERSION/code-server-$VERSION-$OS-$ARCH.tar.gz"
    DOWNLOAD_FILE="$CACHE_DIR/code-server-$VERSION-$OS-$ARCH.tar.gz"
    
    echo "⬇️  Downloading code-server v$VERSION for $OS-$ARCH..."
    echo "   URL: $DOWNLOAD_URL"
    echo "   Tool: $DOWNLOAD_TOOL"
    
    if ! download_file "$DOWNLOAD_URL" "$DOWNLOAD_FILE"; then
        echo "❌ Download failed with $DOWNLOAD_TOOL" >&2
        exit 1
    fi
    
    echo "📦 Extracting archive..."
    if ! tar -xzf "$DOWNLOAD_FILE" -C "$INSTALL_PREFIX/lib"; then
        echo "❌ Extraction failed" >&2
        exit 1
    fi
    
    echo "🔗 Creating symlinks..."
    mv "$INSTALL_PREFIX/lib/code-server-$VERSION-$OS-$ARCH" "$INSTALL_PREFIX/lib/code-server-$VERSION"
    ln -sf "$INSTALL_PREFIX/lib/code-server-$VERSION/bin/code-server" "$INSTALL_PREFIX/bin/code-server"
    
    # Add to PATH if not already there
    if ! echo "$PATH" | grep -q "$INSTALL_PREFIX/bin"; then
        echo "📝 Adding $INSTALL_PREFIX/bin to PATH in ~/.bashrc"
        echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.bashrc
    fi
    
    echo "✅ code-server installed successfully!"
    echo "📍 Location: $INSTALL_PREFIX/lib/code-server-$VERSION"
    echo "🔗 Binary: $INSTALL_PREFIX/bin/code-server"
}

# Main installation flow
main() {
    detect_platform
    check_download_tool
    get_latest_version
    install_standalone
    
    # Verify installation
    if "$HOME/.local/bin/code-server" --version >/dev/null 2>&1; then
        echo "✅ Installation verified successfully"
    else
        echo "⚠️  Installation completed but verification failed"
        echo "   You may need to restart your shell or run: source ~/.bashrc"
    fi
}

main "$@"
"""

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

    def get_install_log(self) -> str:
        """Get the content of the installation log for debugging."""
        try:
            install_log = os.path.expanduser("~/.rayssh/code-server-install.log")
            if os.path.exists(install_log):
                with open(install_log, "r") as f:
                    return f.read()
            else:
                return "No installation log found"
        except Exception as e:
            return f"Error reading installation log: {e}"

    def get_platform_info(self) -> Dict:
        """Return normalized platform info for this target node (os, arch)."""
        try:
            sys_name = platform.system().lower()
            mach = platform.machine().lower()
            if sys_name == "darwin":
                os_name = "macos"
            elif sys_name == "linux":
                os_name = "linux"
            else:
                os_name = sys_name
            if mach in ("x86_64", "amd64"):
                arch = "amd64"
            elif mach in ("aarch64", "arm64"):
                arch = "arm64"
            else:
                arch = mach
            return {"os": os_name, "arch": arch}
        except Exception:
            return {"os": "linux", "arch": "amd64"}

    def get_debug_info(self) -> Dict:
        """Get debug-specific information"""
        info = self.get_info()
        if isinstance(info, dict):
            info["debug_enabled"] = True
            info["debug_env_vars"] = self.debug_env_vars
            # Add Ray dashboard information if available
            if "RAY_DASHBOARD_HOST" in self.env and "RAY_DASHBOARD_PORT" in self.env:
                info["ray_dashboard_url"] = (
                    f"http://{self.env['RAY_DASHBOARD_HOST']}:{self.env['RAY_DASHBOARD_PORT']}"
                )
            elif getattr(self, "debug_config", None):
                url = self.debug_config.get("ray_dashboard_url")
                if url:
                    info["ray_dashboard_url"] = url
        return info
