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
                    f"~/.rayssh/code-server-{timestamp}.log"
                )

                self.host_ip = _detect_accessible_ip()

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

                # Quote root
                quoted_root = quote_shell_single(launch_root)

                # Ensure we can find code-server (check multiple locations)
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
                    "cmd": cmd,
                    "log_file": self.log_file_path,
                    "pid": self.running_process.pid,
                    "host_ip": self.host_ip,
                    "root_dir": self.root_dir,
                    "launch_root": launch_root,
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
        """Check if code-server is available."""
        return self._is_code_server_installed()

    def _install_code_server(self) -> Dict:
        """Install code-server from shipped binary or fallback to download."""
        try:
            # First check if we have a shipped code-server archive
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
            # Check current working directory for code-server archives
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

            # Extract version from filename
            filename = os.path.basename(archive_path)
            # Expected format: code-server-X.X.X-linux-amd64.tar.gz
            parts = filename.replace(".tar.gz", "").split("-")
            if len(parts) >= 3:
                version = parts[2]  # code-server-X.X.X-...
            else:
                version = "unknown"

            print(f"📦 Extracting code-server archive...")
            import tarfile

            with tarfile.open(archive_path, "r:gz") as tar:
                tar.extractall(lib_dir)

            # Find the extracted directory
            extracted_dirs = [
                d for d in os.listdir(lib_dir) if d.startswith("code-server-")
            ]
            if not extracted_dirs:
                return {
                    "success": False,
                    "error": "No extracted code-server directory found",
                }

            extracted_dir = extracted_dirs[0]  # Take the first one
            full_extracted_path = os.path.join(lib_dir, extracted_dir)

            # Rename to consistent directory name
            target_dir = os.path.join(lib_dir, f"code-server-{version}")
            if full_extracted_path != target_dir:
                if os.path.exists(target_dir):
                    import shutil

                    shutil.rmtree(target_dir)
                os.rename(full_extracted_path, target_dir)

            # Create symlink to binary
            binary_source = os.path.join(target_dir, "bin", "code-server")
            binary_target = os.path.join(bin_dir, "code-server")

            if not os.path.exists(binary_source):
                return {
                    "success": False,
                    "error": f"code-server binary not found in {binary_source}",
                }

            # Remove existing symlink if it exists
            if os.path.exists(binary_target) or os.path.islink(binary_target):
                os.remove(binary_target)

            print(f"🔗 Creating symlink...")
            os.symlink(binary_source, binary_target)

            # Make sure it's executable
            os.chmod(binary_source, 0o755)

            print(f"✅ code-server installed successfully!")
            print(f"📍 Location: {target_dir}")
            print(f"🔗 Binary: {binary_target}")

            # Verify installation
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
            # Create installation script from embedded content
            script_path = os.path.expanduser("~/.rayssh/install_code_server.sh")
            os.makedirs(os.path.dirname(script_path), exist_ok=True)

            # Write the installation script content
            script_content = self._get_install_script_content()
            with open(script_path, "w") as f:
                f.write(script_content)
            os.chmod(script_path, 0o755)

            # Create installation log file
            install_log = os.path.expanduser("~/.rayssh/code-server-install.log")

            print(f"🔧 Installing code-server via download (log: {install_log})")

            # Run installation with logging
            with open(install_log, "w") as log_file:
                process = subprocess.Popen(
                    ["bash", script_path, "--method", "standalone"],
                    stdout=log_file,
                    stderr=subprocess.STDOUT,
                    text=True,
                )

                # Wait for installation to complete
                process.wait()

                if process.returncode == 0:
                    print("✅ code-server installation completed successfully")
                    return {"success": True}
                else:
                    print(
                        f"❌ code-server installation failed (exit code: {process.returncode})"
                    )
                    # Read last few lines of log for error context
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

    def _get_install_script_content(self) -> str:
        """Return the embedded installation script content."""
        # Simplified installation script focusing on standalone method using wget instead of curl
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
            # Map to code-server naming
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
