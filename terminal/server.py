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

from utils import detect_accessible_ip
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
        print(f"Terminal client connected from {websocket.remote_address} with path: {path}")

        # Parse session ID and working directory from WebSocket path (RESTful style)
        from urllib.parse import urlparse, parse_qs
        
        session_id = None
        session_working_dir = None
        
        if path:
            try:
                parsed = urlparse(path)
                if parsed.query:
                    query_params = parse_qs(parsed.query)
                    
                    # Extract session ID
                    session_id_list = query_params.get('session_id', [])
                    if session_id_list and session_id_list[0]:
                        session_id = session_id_list[0]
                    
                    # Extract working directory
                    workdir_list = query_params.get('workdir', [])
                    if workdir_list and workdir_list[0]:
                        session_working_dir = workdir_list[0]
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
            "working_dir": session_working_dir or self.default_working_dir,
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

        # Create PTY
        pty_master, pty_slave = pty.openpty()
        session["pty_master"] = pty_master
        session["pty_slave"] = pty_slave

        # Configure PTY terminal settings for proper signal handling
        configure_pty_for_signals(pty_slave)

        # Start shell process
        # Use per-session working directory, fallback to HOME
        shell_cwd = session.get("working_dir") or os.environ.get("HOME", "/home")

        def setup_child():
            """Set up the child process to properly handle signals."""
            setup_controlling_terminal(pty_slave)

        shell_process = subprocess.Popen(
            [os.environ.get("SHELL", "/bin/bash")],
            stdin=pty_slave,
            stdout=pty_slave,
            stderr=pty_slave,
            preexec_fn=setup_child,
            env=(lambda e: (e.update({"TERM": e.get("TERM", "xterm-256color")})) or e)(
                os.environ.copy()
            ),
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
                # Receive message from WebSocket
                message = await websocket.recv()
                data = json.loads(message)

                if data.get("type") == "input":
                    # Write to PTY
                    input_data = data.get("data", "")
                    if isinstance(input_data, str):
                        # Handle Ctrl-C: first try to signal child processes,
                        # if no children, let PTY handle it normally (for line editing)
                        shell_process = session.get("shell_process")
                        if ord(input_data) == 3 and shell_process:  # Ctrl-C
                            # Try to handle via child process signaling
                            handled = handle_ctrl_c_signal(shell_process.pid)
                            if handled:
                                # Signal was sent to children, don't pass Ctrl-C to PTY
                                continue

                        # Preserve control characters (e.g., Ctrl-C = \x03, Ctrl-D = \x04)
                        pty_master = session.get("pty_master")
                        if pty_master:
                            os.write(
                                pty_master, input_data.encode("latin1", errors="ignore")
                            )
                    else:
                        pty_master = session.get("pty_master")
                        if pty_master:
                            os.write(pty_master, bytes(input_data))
                elif data.get("type") == "resize":
                    # Handle terminal resize
                    rows = data.get("rows", 24)
                    cols = data.get("cols", 80)
                    self._resize_pty(session_id, rows, cols)

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
