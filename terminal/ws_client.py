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
import struct
import fcntl
import shutil

import websockets


class TerminalClient:
    """WebSocket client that connects to a remote TerminalActor."""

    def __init__(self):
        self.websocket = None
        self.original_termios = None
        self.running = False
        self.shutdown_requested = False
        self._resize_task = None
        self._last_rows = None
        self._last_cols = None
        self.session_id = None

    async def connect_to_terminal(self, host: str, port: int):
        """Connect to the terminal server and start interactive session."""
        uri = f"ws://{host}:{port}"
        print(f"🔌 Connecting to terminal at {uri}...")

        try:
            # Connect to WebSocket
            self.websocket = await websockets.connect(uri)
            print("✅ Connected! Terminal session started.")

            # Expect an initial hello containing session_id
            try:
                hello = await asyncio.wait_for(self.websocket.recv(), timeout=2.0)
                try:
                    hello_data = json.loads(hello)
                    if isinstance(hello_data, dict) and hello_data.get("type") == "hello":
                        self.session_id = hello_data.get("session_id")
                        if self.session_id:
                            print(f"🔑 Session ID: {self.session_id}")
                except Exception:
                    pass
            except Exception:
                pass

            # Set terminal to raw mode
            self.setup_terminal()
            self.running = True

            # Send initial terminal size
            await self._send_current_winsize()

            # Start resize watcher
            self._resize_task = asyncio.create_task(self._watch_and_send_resize())

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

    def restore_terminal(self):
        """Restore terminal to original mode."""
        if self.original_termios and os.isatty(sys.stdin.fileno()):
            termios.tcsetattr(
                sys.stdin.fileno(), termios.TCSADRAIN, self.original_termios
            )

    def _get_winsize(self):
        """Return (rows, cols) for current terminal, with fallbacks."""
        try:
            s = struct.unpack(
                "hhhh",
                fcntl.ioctl(
                    sys.stdout.fileno(),
                    termios.TIOCGWINSZ,
                    struct.pack("hhhh", 0, 0, 0, 0),
                ),
            )
            rows, cols = int(s[0]), int(s[1])
            if rows > 0 and cols > 0:
                return rows, cols
        except Exception:
            pass
        try:
            size = shutil.get_terminal_size(fallback=(80, 24))
            return size.lines, size.columns
        except Exception:
            return 24, 80

    async def _send_current_winsize(self):
        rows, cols = self._get_winsize()
        self._last_rows, self._last_cols = rows, cols
        payload = {"type": "resize", "rows": rows, "cols": cols}
        if self.session_id:
            payload["session_id"] = self.session_id
        try:
            await self.websocket.send(json.dumps(payload))
        except Exception:
            pass

    async def _watch_and_send_resize(self):
        """Periodically check terminal size and send resize events on change."""
        try:
            while self.running and self.websocket and not self.shutdown_requested:
                rows, cols = self._get_winsize()
                if rows != self._last_rows or cols != self._last_cols:
                    self._last_rows, self._last_cols = rows, cols
                    payload = {"type": "resize", "rows": rows, "cols": cols}
                    if self.session_id:
                        payload["session_id"] = self.session_id
                    try:
                        await self.websocket.send(json.dumps(payload))
                    except Exception:
                        break
                await asyncio.sleep(0.3)
        except asyncio.CancelledError:
            return
        except Exception:
            return

    async def handle_input(self):
        """Read from stdin and send to WebSocket."""
        loop = asyncio.get_event_loop()

        while self.running and self.websocket and not self.shutdown_requested:
            try:
                # Read single character from stdin
                char = await loop.run_in_executor(None, sys.stdin.read, 1)
                if char:
                    # Send to terminal (including control chars like \x03 for Ctrl-C)
                    payload = {"type": "input", "data": char}
                    if self.session_id:
                        payload["session_id"] = self.session_id
                    await self.websocket.send(json.dumps(payload))
                else:
                    # EOF on stdin (Ctrl-D). Do not close the session; stop reading input
                    # and let the server/output dictate when to close.
                    break
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"Input error: {e}")
                break

    async def handle_output(self):
        """Receive from WebSocket and write to stdout."""
        while self.running and self.websocket and not self.shutdown_requested:
            try:
                message = await self.websocket.recv()

                # Support binary frames with RSHT header carrying session_id
                if isinstance(message, (bytes, bytearray)):
                    buf = bytes(message)
                    if len(buf) >= 37 and buf[:4] == b"RSHT" and buf[4] == 1:
                        sid_bytes = buf[5:37]
                        try:
                            sid_str = sid_bytes.decode("ascii")
                        except Exception:
                            sid_str = None
                        if self.session_id is None:
                            self.session_id = sid_str
                        if self.session_id is not None and sid_str is not None and sid_str != self.session_id:
                            continue
                        payload = buf[37:]
                    else:
                        # No header; treat entire payload as PTY bytes
                        payload = buf
                    try:
                        os.write(sys.stdout.fileno(), payload)
                    except Exception:
                        sys.stdout.buffer.write(payload)
                        sys.stdout.flush()
                    continue

                # JSON control messages (e.g., hello)
                data = json.loads(message)
                if data.get("type") == "hello" and not self.session_id:
                    sid = data.get("session_id")
                    if isinstance(sid, str):
                        self.session_id = sid

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

        # Stop resize watcher
        if self._resize_task:
            try:
                self._resize_task.cancel()
                await asyncio.gather(self._resize_task, return_exceptions=True)
            except Exception:
                pass
            finally:
                self._resize_task = None

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
