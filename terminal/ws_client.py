#!/usr/bin/env python3
"""
WebSocket client for RaySSH terminal communication.
"""

import asyncio
import json
import os
import sys
import termios
import tty
import shutil
import signal

import websockets


class TerminalClient:
    """WebSocket client that connects to a remote TerminalActor."""

    def __init__(self):
        self.websocket = None
        self.original_termios = None
        self.running = False
        self.shutdown_requested = False
        self.loop = None

    async def connect_to_terminal(self, host: str, port: int):
        """Connect to the terminal server and start interactive session."""
        uri = f"ws://{host}:{port}"
        print(f"🔌 Connecting to terminal at {uri}...")
        self.loop = asyncio.get_running_loop()

        try:
            # Connect to WebSocket
            self.websocket = await websockets.connect(uri)
            print("✅ Connected! Terminal session started.")

            # Set terminal to raw mode
            self.setup_terminal()
            self.running = True

            # Send initial size
            self._send_resize()

            # Start bidirectional communication
            input_task = asyncio.create_task(self.handle_input())
            output_task = asyncio.create_task(self.handle_output())

            # Keep session running until the server/websocket closes (output task ends)
            await output_task

            # Ensure input task is stopped if still running
            if not input_task.done():
                input_task.cancel()
                try:
                    await input_task
                except asyncio.CancelledError:
                    pass

        except websockets.exceptions.ConnectionClosed:
            # Ensure terminal state is restored before printing any further output
            try:
                self.running = False
                self.shutdown_requested = True
                await self.cleanup()
            finally:
                print("\nConnection closed by server.")
        except Exception as e:
            print(f"Connection error: {e}")
        finally:
            await self.cleanup()

    def setup_terminal(self):
        """Set terminal to raw mode for interactive use."""
        if os.isatty(sys.stdin.fileno()):
            self.original_termios = termios.tcgetattr(sys.stdin.fileno())
            tty.setraw(sys.stdin.fileno())

            # Handle terminal resize
            signal.signal(signal.SIGWINCH, self._sigwinch_handler)

    def _sigwinch_handler(self, signum, frame):
        """Signal handler for SIGWINCH."""
        if self.loop and not self.loop.is_closed():
            self.loop.call_soon_threadsafe(self._send_resize)

    def _send_resize(self):
        """Send terminal resize message."""
        if self.websocket and self.running:
            try:
                cols, rows = shutil.get_terminal_size()
                asyncio.create_task(
                    self.websocket.send(
                        json.dumps({"type": "resize", "rows": rows, "cols": cols})
                    )
                )
            except Exception:
                # Can happen if the socket is closing
                pass

    def restore_terminal(self):
        """Restore terminal to original mode."""
        if self.original_termios and os.isatty(sys.stdin.fileno()):
            termios.tcsetattr(
                sys.stdin.fileno(), termios.TCSADRAIN, self.original_termios
            )
            # Restore default SIGWINCH handler
            signal.signal(signal.SIGWINCH, signal.SIG_DFL)

    async def handle_input(self):
        """Read from stdin and send to WebSocket."""
        queue = asyncio.Queue()

        def on_stdin_ready():
            """Callback for when stdin has data."""
            try:
                data = sys.stdin.read(1024)
                if data:
                    queue.put_nowait(data)
                else:
                    # EOF
                    queue.put_nowait(None)
            except Exception:
                queue.put_nowait(None)

        self.loop.add_reader(sys.stdin.fileno(), on_stdin_ready)

        try:
            while self.running and self.websocket and not self.shutdown_requested:
                data = await queue.get()
                if data is None:
                    # EOF on stdin (Ctrl-D).
                    break

                await self.websocket.send(json.dumps({"type": "input", "data": data}))
        except asyncio.CancelledError:
            pass
        except Exception as e:
            print(f"Input error: {e}")
        finally:
            self.loop.remove_reader(sys.stdin.fileno())

    async def handle_output(self):
        """Receive from WebSocket and write to stdout."""
        while self.running and self.websocket and not self.shutdown_requested:
            try:
                message = await self.websocket.recv()
                data = json.loads(message)

                if data.get("type") == "output":
                    # Write to stdout
                    sys.stdout.write(data.get("data", ""))
                    sys.stdout.flush()

            except websockets.exceptions.ConnectionClosed:
                # Signal shutdown and exit loop; cleanup will restore terminal
                self.running = False
                self.shutdown_requested = True
                break
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"Output error: {e}")
                break

    def request_shutdown(self):
        """Request graceful shutdown."""
        self.shutdown_requested = True
        self.running = False

    async def cleanup(self):
        """Clean up connection and terminal gracefully."""
        self.running = False
        self.shutdown_requested = True

        # Restore terminal first
        try:
            self.restore_terminal()
        except Exception:
            pass

        # Close WebSocket connection gracefully
        if self.websocket:
            try:
                await self.websocket.close()
            except Exception as e:
                print(f"Warning: Error closing WebSocket: {e}")
