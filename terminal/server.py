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
from typing import Dict, Optional, Any

import ray
import websockets
from websockets.server import serve
import uuid

from utils import detect_accessible_ip
from .utils import (
    configure_pty_for_signals,
    setup_controlling_terminal,
    handle_ctrl_c_signal,
)


@ray.remote(num_gpus=0)
class TerminalActor:
    """
    Ray actor that provides a WebSocket-based terminal interface.
    Runs a PTY session and communicates via WebSocket on ports 8080-8081.
    """

    def __init__(self, working_dir: str = None):
        """Initialize the terminal actor."""
        self.pty_master = None
        self.pty_slave = None
        self.shell_process = None
        self.websocket_server = None
        self.websocket = None
        self.running = False
        self.port = None
        self.node_info = None
        self.working_dir = working_dir
        self.session_id = None
        # Map of session_id -> connection/session state
        self.sessions: Dict[str, Dict[str, Any]] = {}
        # Sessions are created lazily per client connection

    def _ensure_session_entry(self, session_id: str) -> None:
        if session_id not in self.sessions:
            self.sessions[session_id] = {
                "websocket": None,
                "pty_master": None,
                "pty_slave": None,
                "shell_pid": None,
                "shell_process": None,
                "created_at": time.time(),
                "tasks": {},
            }

    def close_session(self, session_id: str) -> None:
        """Terminate and cleanup a specific session."""
        sess = self.sessions.get(session_id)
        if not sess:
            return

        # Cancel tasks
        tasks = sess.get("tasks", {}) or {}
        for t in list(tasks.values()):
            try:
                if t and not t.done():
                    t.cancel()
            except Exception:
                pass
        sess["tasks"] = {}

        # Close WebSocket
        ws = sess.get("websocket")
        if ws:
            try:
                asyncio.create_task(ws.close())
            except Exception:
                pass
        sess["websocket"] = None

        # Terminate shell process
        shell_process = sess.get("shell_process")
        if shell_process:
            try:
                os.killpg(os.getpgid(shell_process.pid), signal.SIGTERM)
                shell_process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(os.getpgid(shell_process.pid), signal.SIGKILL)
                    shell_process.wait(timeout=1)
                except Exception:
                    pass
            except Exception:
                pass
        sess["shell_process"] = None

        # Close PTY master
        if sess.get("pty_master"):
            try:
                os.close(sess["pty_master"])
            except Exception:
                pass
        sess["pty_master"] = None
        sess["pty_slave"] = None

        # Remove session entry
        try:
            del self.sessions[session_id]
        except Exception:
            pass

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
        print(f"Terminal client connected from {websocket.remote_address}")
        # Create a new session id for this connection
        sid = uuid.uuid4().hex
        self._ensure_session_entry(sid)
        self.sessions[sid]["websocket"] = websocket

        try:
            # Send initial hello with session_id
            try:
                ws = self.sessions.get(sid, {}).get("websocket")
                if ws:
                    await ws.send(
                        json.dumps({
                            "type": "hello",
                            "session_id": sid,
                        })
                    )
            except Exception:
                pass

            # Start PTY if not already running
            if not self.sessions.get(sid, {}).get("shell_process"):
                await self.start_pty(sid)

            # Create tasks for bidirectional communication
            pty_to_ws_task = asyncio.create_task(self.pty_to_websocket(sid))
            ws_to_pty_task = asyncio.create_task(self.websocket_to_pty(sid))
            self.sessions[sid]["tasks"]["pty_to_ws"] = pty_to_ws_task
            self.sessions[sid]["tasks"]["ws_to_pty"] = ws_to_pty_task

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
            if sid in self.sessions:
                # Ensure the session is fully closed
                self.close_session(sid)

    async def start_pty(self, session_id: str):
        """Start the PTY session with a shell."""
        # Create PTY
        pty_master, pty_slave = pty.openpty()

        # Configure PTY terminal settings for proper signal handling
        try:
            if configure_pty_for_signals(pty_slave):
                print("✓ PTY configured for normal mode with signal handling")
            else:
                print("Warning: Could not configure PTY attributes")
        except Exception as e:
            print(f"Warning: PTY configuration error: {e}")

        # Start shell process
        # If no working_dir specified, use HOME directory (remote's HOME)
        shell_cwd = (
            self.working_dir if self.working_dir else os.environ.get("HOME", "/home")
        )

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

        # Close slave fd in parent (shell process has it)
        os.close(pty_slave)

        print(f"Started shell process {shell_process.pid}")
        if self.working_dir:
            print(f"Shell started in working directory: {self.working_dir}")
        else:
            print(f"Shell started in HOME directory: {shell_cwd}")

        # Update session store
        self._ensure_session_entry(session_id)
        sess = self.sessions[session_id]
        sess["pty_master"] = pty_master
        sess["pty_slave"] = None  # closed in parent
        sess["shell_process"] = shell_process
        sess["shell_pid"] = shell_process.pid

    async def pty_to_websocket(self, session_id: str):
        """Forward data from PTY to WebSocket."""
        loop = asyncio.get_event_loop()

        while self.running:
            try:
                # Use asyncio to read from PTY (non-blocking)
                data = await loop.run_in_executor(None, self._read_pty, session_id)
                if data:
                    # Send binary frame with small header carrying session_id
                    # Header format: b'RSHT' + version(1 byte) + session_id(32 ascii hex)
                    ws = self.sessions.get(session_id, {}).get("websocket")
                    if not ws:
                        # No connected websocket; stop forwarding
                        break
                    header = b"RSHT" + bytes([1]) + session_id.encode("ascii")
                    await ws.send(header + data)
                else:
                    # PTY closed; cleanup session
                    self.close_session(session_id)
                    break
            except Exception as e:
                print(f"Error reading from PTY: {e}")
                # Cleanup and exit this session loop
                self.close_session(session_id)
                break

    def _read_pty(self, session_id: str):
        """Read data from PTY (blocking operation for executor)."""
        try:
            sess = self.sessions.get(session_id)
            if not sess or not sess.get("pty_master"):
                return None
            return os.read(sess["pty_master"], 1024)
        except OSError:
            return None

    async def websocket_to_pty(self, session_id: str):
        """Forward data from WebSocket to PTY."""
        while self.running:
            try:
                # Receive message from WebSocket
                ws = self.sessions.get(session_id, {}).get("websocket")
                if not ws:
                    break
                message = await ws.recv()
                data = json.loads(message)

                # Validate session_id if present
                sid = data.get("session_id")
                if sid is not None and sid != session_id:
                    # Ignore messages for a different session
                    continue

                if data.get("type") == "input":
                    # Write to PTY
                    input_data = data.get("data", "")
                    if isinstance(input_data, str):
                        # Handle Ctrl-C: first try to signal child processes, 
                        # if no children, let PTY handle it normally (for line editing)
                        sess = self.sessions.get(session_id)
                        shell_process = sess.get("shell_process") if sess else None
                        if ord(input_data) == 3 and shell_process:  # Ctrl-C
                            # Try to handle via child process signaling
                            handled = handle_ctrl_c_signal(shell_process.pid)
                            if handled:
                                # Signal was sent to children, don't pass Ctrl-C to PTY
                                continue

                        # Preserve control characters (e.g., Ctrl-C = \x03, Ctrl-D = \x04)
                        sess = self.sessions.get(session_id)
                        if not sess or not sess.get("pty_master"):
                            continue
                        os.write(sess["pty_master"], input_data.encode("latin1", errors="ignore"))
                    else:
                        sess = self.sessions.get(session_id)
                        if not sess or not sess.get("pty_master"):
                            continue
                        os.write(sess["pty_master"], bytes(input_data))
                elif data.get("type") == "resize":
                    # Handle terminal resize
                    rows = data.get("rows", 24)
                    cols = data.get("cols", 80)
                    self._resize_pty(session_id, rows, cols)

            except websockets.exceptions.ConnectionClosed:
                # Client disconnected; cleanup this session only
                self.close_session(session_id)
                break
            except Exception as e:
                print(f"Error processing WebSocket message: {e}")
                self.close_session(session_id)
                break

    def _resize_pty(self, session_id: str, rows: int, cols: int):
        """Resize the PTY."""
        try:
            import struct
            import fcntl
            import termios

            # Set terminal size
            winsize = struct.pack("HHHH", rows, cols, 0, 0)
            sess = self.sessions.get(session_id)
            if not sess or not sess.get("pty_master"):
                return
            fcntl.ioctl(sess["pty_master"], termios.TIOCSWINSZ, winsize)

            # Send SIGWINCH to shell process
            sess = self.sessions.get(session_id)
            shell_process = sess.get("shell_process") if sess else None
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

                print(f"Terminal server started on port {port}")
                self.node_info = self.get_node_info()

                # Server is now running, return success
                return True

            except OSError as e:
                if "Address already in use" in str(e) and port != ports_to_try[-1]:
                    print(f"Port {port} in use, trying next port...")
                    continue
                else:
                    raise

    def stop_server(self):
        """Stop the WebSocket server and clean up gracefully."""
        print("Stopping terminal server...")
        self.running = False

        # Close WebSocket server
        if self.websocket_server:
            try:
                self.websocket_server.close()
                print("✓ WebSocket server closed")
            except Exception as e:
                print(f"Warning: Error closing WebSocket server: {e}")

        # Close all active sessions
        for sid in list(self.sessions.keys()):
            try:
                self.close_session(sid)
            except Exception as e:
                print(f"Warning: Error closing session {sid}: {e}")

    def get_server_info(self) -> Dict:
        """Get server connection information."""
        return self.node_info

    # Ray remote methods (async methods need to be wrapped)
    def start_terminal_server(self, port: int = None):
        """Start the terminal server (Ray remote method)."""

        # Run the async server in a new event loop
        def run_server():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                # Start the server
                loop.run_until_complete(self.start_server(port))
                # Keep the server running
                if self.websocket_server:
                    loop.run_until_complete(self.websocket_server.wait_closed())
            except Exception as e:
                print(f"Server error: {e}")
            finally:
                loop.close()

        # Start server in a separate thread (not daemon so it keeps running)
        server_thread = threading.Thread(target=run_server, daemon=False)
        server_thread.start()

        # Wait for server to start
        time.sleep(2)

        return self.get_server_info()

    def stop_terminal_server(self):
        """Stop the terminal server (Ray remote method)."""
        self.stop_server()
        return True
