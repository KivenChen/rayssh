#!/usr/bin/env python3
"""
Ray-native terminal implementation using WebSocket communication.
The TerminalActor runs on the target node and provides a shell interface via WebSocket.
"""

import asyncio
import json
import os
import pty
import signal
import subprocess
import threading
import time
from typing import Dict, Optional

import ray
import websockets
from websockets.server import serve

from utils import detect_accessible_ip, get_sync_editor_warning_message
from .utils import (
    configure_pty_for_signals,
    setup_controlling_terminal,
    handle_ctrl_c_signal,
)


@ray.remote(num_gpus=0)
class WorkdirActor:
    """
    Simple actor to resolve Ray-managed working directory.
    Created with runtime_env={"working_dir": path} to get the remote path.
    """

    def get_working_dir(self):
        """Get the current working directory (Ray-managed)"""
        import os

        return os.getcwd()


@ray.remote
class GPUDaemonActor:
    """
    GPU daemon actor that allocates GPUs and provides CUDA_VISIBLE_DEVICES to shell processes.
    This actor's only job is to occupy GPU resources and make them available to shell processes.
    """

    def __init__(self, session_id: str):
        self.session_id = session_id
        self.cuda_visible_devices = None
        self._initialize_gpu_env()

    def _initialize_gpu_env(self):
        """Initialize and capture GPU environment variables."""
        import os

        self.cuda_visible_devices = os.environ.get("CUDA_VISIBLE_DEVICES", "")
        print(
            f"🎛️ GPU daemon for session {self.session_id}: CUDA_VISIBLE_DEVICES={self.cuda_visible_devices}"
        )

    def get_gpu_env(self):
        """Get GPU environment variables for shell process."""
        import os

        return {
            "CUDA_VISIBLE_DEVICES": self.cuda_visible_devices or "",
            "RAY_SESSION_GPUS": str(self.get_allocated_gpu_count()),
            "RAY_SESSION_ID": self.session_id,
        }

    def get_allocated_gpu_count(self):
        """Get the number of allocated GPUs."""
        if not self.cuda_visible_devices:
            return 0
        return len(
            [x.strip() for x in self.cuda_visible_devices.split(",") if x.strip()]
        )

    def ping(self):
        """Keep-alive method to ensure actor stays active."""
        return f"GPU daemon for session {self.session_id} is alive"

    def get_node_info(self):
        """Get the node ID and IP where this GPU daemon actor is running."""
        import ray

        node_id = ray.get_runtime_context().get_node_id()
        node_ip = ray.util.get_node_ip_address()

        return {"node_id": node_id, "node_ip": node_ip}


@ray.remote(num_gpus=0)
class TerminalActor:
    """
    Ray actor that provides a WebSocket-based terminal interface.
    Runs a PTY session and communicates via WebSocket on ports 8080-8081.
    """

    def __init__(self, working_dir: str = None):
        """Initialize the terminal actor."""
        self.websocket_server = None
        self.running = False
        self.port = None
        self.node_info = None
        self.default_working_dir = working_dir
        self.sessions = {}  # session_id -> {pty_master, pty_slave, shell_process, websocket, working_dir}

    def get_node_info(self) -> Dict:
        """Get information about the current node."""
        return {
            "hostname": os.uname().nodename,
            "ip": detect_accessible_ip(),
            "cwd": os.getcwd(),
            "pid": os.getpid(),
            "user": os.environ.get("USER", "unknown"),
            "port": self.port,
        }

    async def handle_client(self, websocket, path):
        """Handle a WebSocket client connection."""
        print(
            f"Terminal client connected from {websocket.remote_address} with path: {path}"
        )

        # Parse session ID, working directory, and CUDA devices from WebSocket path (RESTful style)
        from urllib.parse import urlparse, parse_qs

        session_id = None
        session_working_dir = None
        session_cuda_visible_devices = None
        session_sync_enabled = False

        if path:
            try:
                parsed = urlparse(path)
                if parsed.query:
                    query_params = parse_qs(parsed.query)

                    # Extract session ID
                    session_id_list = query_params.get("session_id", [])
                    if session_id_list and session_id_list[0]:
                        session_id = session_id_list[0]

                    # Extract working directory
                    workdir_list = query_params.get("workdir", [])
                    if workdir_list and workdir_list[0]:
                        session_working_dir = workdir_list[0]

                    # Extract CUDA_VISIBLE_DEVICES
                    cuda_devices_list = query_params.get("cuda_visible_devices", [])
                    if cuda_devices_list and cuda_devices_list[0]:
                        session_cuda_visible_devices = cuda_devices_list[0]

                    # Extract sync_enabled flag
                    sync_enabled_list = query_params.get("sync_enabled", [])
                    if sync_enabled_list and sync_enabled_list[0] == "true":
                        session_sync_enabled = True
            except Exception as e:
                print(f"Warning: Could not parse WebSocket path '{path}': {e}")

        # Generate session ID if not provided
        if not session_id:
            import uuid

            session_id = uuid.uuid4().hex
        session = {
            "websocket": websocket,
            "pty_master": None,
            "pty_slave": None,
            "shell_process": None,
            "created_at": time.time(),
            # Only use per-session working_dir when explicitly provided by client.
            # Otherwise, fall back to HOME later when starting PTY.
            "working_dir": session_working_dir,
            "cuda_visible_devices": session_cuda_visible_devices,
            "sync_enabled": session_sync_enabled,
        }

        self.sessions[session_id] = session

        try:
            # Start PTY for this session
            await self.start_pty_for_session(session_id)

            # Create tasks for bidirectional communication
            pty_to_ws_task = asyncio.create_task(self.pty_to_websocket(session_id))
            ws_to_pty_task = asyncio.create_task(self.websocket_to_pty(session_id))

            # Wait for either task to complete (client disconnect or PTY exit)
            done, pending = await asyncio.wait(
                [pty_to_ws_task, ws_to_pty_task], return_when=asyncio.FIRST_COMPLETED
            )

            # Cancel remaining tasks
            for task in pending:
                task.cancel()

        except websockets.exceptions.ConnectionClosed:
            print("Terminal client disconnected")
        except Exception as e:
            print(f"Error in terminal session: {e}")
        finally:
            # Clean up this session
            self.cleanup_session(session_id)

    async def start_pty_for_session(self, session_id: str):
        """Start the PTY session with a shell for a specific session."""
        session = self.sessions[session_id]
        session_cuda_visible_devices = session.get("cuda_visible_devices")

        # Prepare GPU environment if CUDA devices are provided by client
        gpu_env = {}
        if session_cuda_visible_devices:
            gpu_count = len(
                [
                    x.strip()
                    for x in session_cuda_visible_devices.split(",")
                    if x.strip()
                ]
            )
            gpu_env = {
                "CUDA_VISIBLE_DEVICES": session_cuda_visible_devices,
                "RAY_SESSION_GPUS": str(gpu_count),
                "RAY_SESSION_ID": session_id,
            }
            print(
                f"🎛️ Session {session_id}: Using GPU environment from client: {gpu_env}"
            )

        # Create PTY
        pty_master, pty_slave = pty.openpty()
        session["pty_master"] = pty_master
        session["pty_slave"] = pty_slave

        # Configure PTY terminal settings for proper signal handling
        configure_pty_for_signals(pty_slave)

        # Start shell process
        # Use per-session working directory if provided; otherwise use HOME
        shell_cwd = session.get("working_dir")
        if not shell_cwd:
            shell_cwd = os.environ.get("HOME", "~")

        def setup_child():
            """Set up the child process to properly handle signals."""
            setup_controlling_terminal(pty_slave)

        # Prepare shell environment with GPU access if requested
        shell_env = os.environ.copy()
        shell_env.update({"TERM": shell_env.get("TERM", "xterm-256color")})

        # Ensure UTF-8 locale so multibyte characters (e.g., Chinese) are handled correctly
        try:
            has_utf8 = any(
                isinstance(shell_env.get(k), str) and "UTF-8" in shell_env.get(k)
                for k in ["LANG", "LC_ALL", "LC_CTYPE"]
            )
            if not has_utf8:
                # Prefer C.UTF-8 for broad availability
                shell_env.setdefault("LANG", "C.UTF-8")
                shell_env.setdefault("LC_CTYPE", "C.UTF-8")
                shell_env.setdefault("LC_ALL", "C.UTF-8")
        except Exception:
            # Best-effort; ignore if environment manipulation fails
            pass

        # Add GPU environment variables from daemon actor
        if gpu_env:
            shell_env.update(gpu_env)

        # Determine shell command - use wrapper for sync sessions
        session = self.sessions[session_id]
        if session.get("sync_enabled", False):
            # Create and use shell wrapper for sync sessions
            shell_wrapper_path = self._create_sync_shell_wrapper(session_id)
            shell_command = [shell_wrapper_path]
            session["shell_wrapper_path"] = shell_wrapper_path
            print(f"🔄 Starting sync session with editor warnings enabled")
        else:
            # Normal shell for regular sessions
            shell_command = [os.environ.get("SHELL", "/bin/bash")]

        shell_process = subprocess.Popen(
            shell_command,
            stdin=pty_slave,
            stdout=pty_slave,
            stderr=pty_slave,
            preexec_fn=setup_child,
            env=shell_env,
            cwd=shell_cwd,
        )

        session["shell_process"] = shell_process

        # Close slave fd in parent (shell process has it)
        os.close(pty_slave)
        session["pty_slave"] = None

        print(f"Started shell process {shell_process.pid}")

    def cleanup_session(self, session_id: str):
        """Clean up a specific session."""
        if session_id not in self.sessions:
            return

        session = self.sessions[session_id]

        # Close WebSocket
        if session.get("websocket"):
            try:
                asyncio.create_task(session["websocket"].close())
            except Exception:
                pass

        # Terminate shell process
        shell_process = session.get("shell_process")
        if shell_process:
            try:
                os.killpg(os.getpgid(shell_process.pid), signal.SIGTERM)
                shell_process.wait(timeout=2)
            except Exception:
                try:
                    os.killpg(os.getpgid(shell_process.pid), signal.SIGKILL)
                    shell_process.wait(timeout=1)
                except Exception:
                    pass

        # GPU daemon actor is managed by the client, no cleanup needed here

        # Clean up shell wrapper script if it exists
        shell_wrapper_path = session.get("shell_wrapper_path")
        if shell_wrapper_path:
            try:
                os.unlink(shell_wrapper_path)
            except Exception:
                pass

        # Close PTY
        pty_master = session.get("pty_master")
        if pty_master:
            try:
                os.close(pty_master)
            except Exception:
                pass

        # Remove session
        del self.sessions[session_id]

    async def pty_to_websocket(self, session_id: str):
        """Forward data from PTY to WebSocket for a specific session."""
        loop = asyncio.get_event_loop()

        while self.running and session_id in self.sessions:
            session = self.sessions[session_id]
            websocket = session.get("websocket")
            shell_process = session.get("shell_process")

            if not websocket:
                break

            try:
                # Check if shell process is still alive
                if shell_process and shell_process.poll() is not None:
                    print("Shell process ended")
                    break

                # Use asyncio to read from PTY (non-blocking)
                data = await loop.run_in_executor(None, self._read_pty, session_id)
                if data:
                    # Send raw bytes to WebSocket client (binary frame)
                    await websocket.send(data)
                else:
                    # PTY closed
                    break

            except websockets.exceptions.ConnectionClosed:
                print("WebSocket closed")
                break
            except Exception as e:
                print(f"Error forwarding PTY to WebSocket: {e}")
                break

    def _read_pty(self, session_id: str):
        """Read data from PTY (blocking operation for executor)."""
        try:
            session = self.sessions.get(session_id)
            if not session or not session.get("pty_master"):
                return None
            return os.read(session["pty_master"], 1024)
        except OSError:
            return None

    async def websocket_to_pty(self, session_id: str):
        """Forward data from WebSocket to PTY for a specific session."""
        while self.running and session_id in self.sessions:
            session = self.sessions[session_id]
            websocket = session.get("websocket")

            if not websocket:
                break

            try:
                # Receive message from WebSocket (can be binary or text-json)
                message = await websocket.recv()

                # Binary frames: raw PTY input bytes
                if isinstance(message, (bytes, bytearray)):
                    pty_master = session.get("pty_master")
                    if pty_master:
                        os.write(pty_master, bytes(message))
                    continue

                # Text frames: JSON control messages (e.g., resize, sync)
                data = json.loads(message)
                message_type = data.get("type")

                if message_type == "resize":
                    rows = data.get("rows", 24)
                    cols = data.get("cols", 80)
                    self._resize_pty(session_id, rows, cols)
                elif message_type == "sync_put":
                    await self._handle_sync_put(session_id, data)
                elif message_type == "sync_del":
                    await self._handle_sync_del(session_id, data)
                elif message_type == "sync_move":
                    await self._handle_sync_move(session_id, data)

            except websockets.exceptions.ConnectionClosed:
                # Client disconnected; this session ends but server keeps running
                break
            except Exception as e:
                print(f"Error processing WebSocket message: {e}")
                break

    def _resize_pty(self, session_id: str, rows: int, cols: int):
        """Resize the PTY for a specific session."""
        try:
            session = self.sessions.get(session_id)
            if not session:
                return

            import struct
            import fcntl
            import termios

            # Set terminal size
            winsize = struct.pack("HHHH", rows, cols, 0, 0)
            pty_master = session.get("pty_master")
            if pty_master:
                fcntl.ioctl(pty_master, termios.TIOCSWINSZ, winsize)

            # Send SIGWINCH to shell process
            shell_process = session.get("shell_process")
            if shell_process:
                os.killpg(os.getpgid(shell_process.pid), signal.SIGWINCH)

        except Exception as e:
            print(f"Error resizing PTY: {e}")

    def _create_sync_shell_wrapper(self, session_id: str) -> str:
        """Create a temporary shell wrapper script for sync sessions with editor aliases."""
        import tempfile
        import stat

        # Create temporary shell script
        wrapper_fd, wrapper_path = tempfile.mkstemp(
            suffix=".sh", prefix=f"rayssh_sync_{session_id}_"
        )

        # Get the user's shell
        user_shell = os.environ.get("SHELL", "/bin/bash")

        # Get the warning message from utils
        warning_message = get_sync_editor_warning_message()

        # Shell wrapper content with improved warnings
        wrapper_content = f"""#!/bin/bash

# RaySSH Sync Session - Editor Aliases
show_sync_warning() {{
    echo '{warning_message}'
    read
}}

vim() {{
    show_sync_warning
    command vim "$@"
}}

vi() {{
    show_sync_warning
    command vi "$@"
}}

nano() {{
    show_sync_warning
    command nano "$@"
}}

emacs() {{
    show_sync_warning
    command emacs "$@"
}}

# Start the user's actual shell
exec {user_shell} "$@"
"""

        # Write the wrapper script
        with os.fdopen(wrapper_fd, "w") as f:
            f.write(wrapper_content)

        # Make it executable
        os.chmod(wrapper_path, stat.S_IRWXU | stat.S_IRGRP | stat.S_IROTH)

        return wrapper_path

    def _validate_sync_session(
        self, session_id: str, data: dict, operation: str
    ) -> bool:
        """Validate session ID matches for sync operations."""
        msg_session_id = data.get("session_id")
        if msg_session_id and msg_session_id != session_id:
            print(
                f"Warning: Session ID mismatch in {operation}: {msg_session_id} != {session_id}"
            )
            return False
        return True

    def _get_session_base_dir(self, session_id: str, operation: str) -> str:
        """Get the base directory for a session, with error handling."""
        session = self.sessions.get(session_id)
        if not session:
            print(f"Error: Session {session_id} not found for {operation}")
            return None
        return session.get("working_dir") or os.getcwd()

    def _resolve_and_validate_path(
        self, base_dir: str, relpath: str, operation: str
    ) -> str:
        """Resolve relative path and validate it's within base directory."""
        if not relpath:
            print(f"Error: {operation} missing relpath")
            return None

        full_path = os.path.join(base_dir, relpath)
        full_path = os.path.abspath(full_path)
        base_dir = os.path.abspath(base_dir)

        if not full_path.startswith(base_dir):
            print(f"Error: {operation} path outside working directory: {relpath}")
            return None

        return full_path

    def _ensure_parent_directory(self, full_path: str):
        """Create parent directories if they don't exist."""
        parent_dir = os.path.dirname(full_path)
        if parent_dir and not os.path.exists(parent_dir):
            os.makedirs(parent_dir, exist_ok=True)

    async def _handle_sync_put(self, session_id: str, data: dict):
        """Handle sync_put message - create or update a file."""
        try:
            # Validate session and get paths
            if not self._validate_sync_session(session_id, data, "sync_put"):
                return

            base_dir = self._get_session_base_dir(session_id, "sync_put")
            if not base_dir:
                return

            relpath = data.get("relpath")
            full_path = self._resolve_and_validate_path(base_dir, relpath, "sync_put")
            if not full_path:
                return

            # Create parent directories if needed
            self._ensure_parent_directory(full_path)

            # Decode and write content
            content = data.get("content", "")
            encoding = data.get("encoding", "base64")

            if encoding == "base64":
                import base64

                file_data = base64.b64decode(content.encode("ascii"))
            else:
                file_data = content.encode("utf-8")

            with open(full_path, "wb") as f:
                f.write(file_data)

            # Set file mode if provided
            mode = data.get("mode")
            if mode:
                try:
                    os.chmod(full_path, int(mode, 8))
                except Exception as e:
                    print(f"Warning: Could not set file mode {mode}: {e}")

            print(f"📁 Sync: Updated {relpath} ({len(file_data)} bytes)")

        except Exception as e:
            print(f"Error handling sync_put: {e}")

    async def _handle_sync_del(self, session_id: str, data: dict):
        """Handle sync_del message - delete a file."""
        try:
            # Validate session and get paths
            if not self._validate_sync_session(session_id, data, "sync_del"):
                return

            base_dir = self._get_session_base_dir(session_id, "sync_del")
            if not base_dir:
                return

            relpath = data.get("relpath")
            full_path = self._resolve_and_validate_path(base_dir, relpath, "sync_del")
            if not full_path:
                return

            # Delete file if it exists
            if os.path.exists(full_path):
                if os.path.isfile(full_path):
                    os.remove(full_path)
                    print(f"📁 Sync: Deleted {relpath}")
                elif os.path.isdir(full_path):
                    # For directories, only delete if empty (safety)
                    try:
                        os.rmdir(full_path)
                        print(f"📁 Sync: Deleted directory {relpath}")
                    except OSError:
                        print(f"Warning: Directory not empty, skipping: {relpath}")
                else:
                    print(f"Warning: Unknown file type, skipping: {relpath}")
            else:
                print(f"📁 Sync: File already deleted: {relpath}")

        except Exception as e:
            print(f"Error handling sync_del: {e}")

    async def _handle_sync_move(self, session_id: str, data: dict):
        """Handle sync_move message - move/rename a file."""
        try:
            # Validate session and get base directory
            if not self._validate_sync_session(session_id, data, "sync_move"):
                return

            base_dir = self._get_session_base_dir(session_id, "sync_move")
            if not base_dir:
                return

            # Validate both source and destination paths
            src_relpath = data.get("src")
            dst_relpath = data.get("dst")

            if not src_relpath or not dst_relpath:
                print("Error: sync_move missing src or dst")
                return

            src_full_path = self._resolve_and_validate_path(
                base_dir, src_relpath, "sync_move"
            )
            dst_full_path = self._resolve_and_validate_path(
                base_dir, dst_relpath, "sync_move"
            )

            if not src_full_path or not dst_full_path:
                return

            # Create destination parent directory if needed
            self._ensure_parent_directory(dst_full_path)

            # Move file if source exists
            if os.path.exists(src_full_path):
                os.rename(src_full_path, dst_full_path)
                print(f"📁 Sync: Moved {src_relpath} -> {dst_relpath}")
            else:
                print(f"Warning: Source file not found for move: {src_relpath}")

        except Exception as e:
            print(f"Error handling sync_move: {e}")

    async def start_server(self, port: int = None):
        """Start the WebSocket server."""
        # Try ports 8080 and 8081
        ports_to_try = [port] if port else [8080, 8081]

        for port in ports_to_try:
            try:
                self.port = port
                self.running = True

                # Start WebSocket server
                self.websocket_server = await serve(
                    self.handle_client,
                    "0.0.0.0",  # Listen on all interfaces
                    port,
                    ping_interval=20,
                    ping_timeout=10,
                )

                self.node_info = self.get_node_info()
                return True

            except OSError as e:
                if "Address already in use" in str(e):
                    if port == ports_to_try[-1]:
                        # All ports are in use - set up info anyway
                        self.port = port
                        self.running = True
                        self.node_info = self.get_node_info()
                        return True
                    else:
                        continue
                else:
                    raise

    def stop_server(self):
        """Stop the WebSocket server and clean up gracefully."""
        self.running = False

        # Clean up all sessions
        for session_id in list(self.sessions.keys()):
            self.cleanup_session(session_id)

        # Close WebSocket server
        if self.websocket_server:
            try:
                self.websocket_server.close()
            except Exception:
                pass

    # Ray remote methods
    async def start_terminal_server(self, port: int = None):
        """Start the terminal server (Ray remote method)."""

        # Check if server is already running
        if self.running and self.websocket_server and self.port:
            return self.get_node_info()

        # Start WebSocket server
        try:
            success = await self.start_server(port)
            if success:
                return self.get_node_info()
            else:
                return None
        except Exception as e:
            print(f"Server startup error: {e}")
            return None

    def stop_terminal_server(self):
        """Stop the terminal server (Ray remote method)."""
        self.stop_server()
        return True
