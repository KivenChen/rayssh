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


@ray.remote(scheduling_strategy="SPREAD")
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
        self.websocket = websocket

        try:
            # Start PTY if not already running
            if not self.shell_process:
                await self.start_pty()

            # Create tasks for bidirectional communication
            pty_to_ws_task = asyncio.create_task(self.pty_to_websocket())
            ws_to_pty_task = asyncio.create_task(self.websocket_to_pty())

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
            self.websocket = None

    async def start_pty(self):
        """Start the PTY session with a shell."""
        # Create PTY
        self.pty_master, self.pty_slave = pty.openpty()

        # Configure PTY terminal settings for proper signal handling
        if configure_pty_for_signals(self.pty_slave):
            print("✓ PTY configured for normal mode with signal handling")
        else:
            print(f"Warning: Could not configure PTY attributes: {e}")

        # Start shell process
        # If no working_dir specified, use HOME directory (remote's HOME)
        shell_cwd = (
            self.working_dir if self.working_dir else os.environ.get("HOME", "/home")
        )

        def setup_child():
            """Set up the child process to properly handle signals."""
            setup_controlling_terminal(self.pty_slave)

        self.shell_process = subprocess.Popen(
            [os.environ.get("SHELL", "/bin/bash")],
            stdin=self.pty_slave,
            stdout=self.pty_slave,
            stderr=self.pty_slave,
            preexec_fn=setup_child,
            env=(lambda e: (e.update({"TERM": e.get("TERM", "xterm-256color")})) or e)(
                os.environ.copy()
            ),
            cwd=shell_cwd,
        )

        # Close slave fd in parent (shell process has it)
        os.close(self.pty_slave)

        print(f"Started shell process {self.shell_process.pid}")
        if self.working_dir:
            print(f"Shell started in working directory: {self.working_dir}")
        else:
            print(f"Shell started in HOME directory: {shell_cwd}")

    async def pty_to_websocket(self):
        """Forward data from PTY to WebSocket."""
        loop = asyncio.get_event_loop()

        while self.running and self.websocket:
            try:
                # Use asyncio to read from PTY (non-blocking)
                data = await loop.run_in_executor(None, self._read_pty)
                if data:
                    # Send raw bytes to WebSocket client (binary frame)
                    await self.websocket.send(data)
                else:
                    # PTY closed
                    break
            except Exception as e:
                print(f"Error reading from PTY: {e}")
                break

    def _read_pty(self):
        """Read data from PTY (blocking operation for executor)."""
        try:
            return os.read(self.pty_master, 1024)
        except OSError:
            return None

    async def websocket_to_pty(self):
        """Forward data from WebSocket to PTY."""
        while self.running and self.websocket:
            try:
                # Receive message from WebSocket
                message = await self.websocket.recv()
                data = json.loads(message)

                if data.get("type") == "input":
                    # Write to PTY
                    input_data = data.get("data", "")
                    if isinstance(input_data, str):
                        # Handle Ctrl-C by sending SIGINT to child processes
                        if ord(input_data) == 3 and self.shell_process:  # Ctrl-C
                            # Try to handle via child process signaling
                            handle_ctrl_c_signal(self.shell_process.pid)

                        # Preserve control characters (e.g., Ctrl-C = \x03, Ctrl-D = \x04)
                        os.write(
                            self.pty_master,
                            input_data.encode("latin1", errors="ignore"),
                        )
                    else:
                        os.write(self.pty_master, bytes(input_data))
                elif data.get("type") == "resize":
                    # Handle terminal resize
                    rows = data.get("rows", 24)
                    cols = data.get("cols", 80)
                    self._resize_pty(rows, cols)

            except websockets.exceptions.ConnectionClosed:
                # Client disconnected; stop running to allow cleanup
                self.running = False
                break
            except Exception as e:
                print(f"Error processing WebSocket message: {e}")
                break

    def _resize_pty(self, rows: int, cols: int):
        """Resize the PTY."""
        try:
            import struct
            import fcntl
            import termios

            # Set terminal size
            winsize = struct.pack("HHHH", rows, cols, 0, 0)
            fcntl.ioctl(self.pty_master, termios.TIOCSWINSZ, winsize)

            # Send SIGWINCH to shell process
            if self.shell_process:
                os.killpg(os.getpgid(self.shell_process.pid), signal.SIGWINCH)

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

        # Close WebSocket connection if active
        if self.websocket:
            try:
                asyncio.create_task(self.websocket.close())
                print("✓ WebSocket connection closed")
            except Exception as e:
                print(f"Warning: Error closing WebSocket connection: {e}")

        # Clean up shell process gracefully
        if self.shell_process:
            try:
                print("Terminating shell process...")
                # Send SIGTERM first for graceful shutdown
                os.killpg(os.getpgid(self.shell_process.pid), signal.SIGTERM)
                self.shell_process.wait(timeout=5)
                print("✓ Shell process terminated gracefully")
            except subprocess.TimeoutExpired:
                print("Shell process didn't terminate, forcing kill...")
                try:
                    os.killpg(os.getpgid(self.shell_process.pid), signal.SIGKILL)
                    self.shell_process.wait(timeout=2)
                    print("✓ Shell process killed")
                except Exception as e:
                    print(f"Warning: Error killing shell process: {e}")
            except Exception as e:
                print(f"Warning: Error terminating shell process: {e}")

        # Clean up PTY
        if self.pty_master:
            try:
                os.close(self.pty_master)
                print("✓ PTY closed")
            except Exception as e:
                print(f"Warning: Error closing PTY: {e}")

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
