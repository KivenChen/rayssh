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

import websockets


class TerminalClient:
    """WebSocket client that connects to a remote TerminalActor."""

    def __init__(self):
        self.websocket = None
        self.original_termios = None
        self.running = False
        self.shutdown_requested = False

    async def connect_to_terminal(self, host: str, port: int):
        """Connect to the terminal server and start interactive session."""
        uri = f"ws://{host}:{port}"
        print(f"🔌 Connecting to terminal at {uri}...")

        try:
            # Connect to WebSocket
            self.websocket = await websockets.connect(uri)
            print("✅ Connected! Terminal session started.")

            # Set terminal to raw mode
            self.setup_terminal()
            self.running = True

            # Start bidirectional communication
            input_task = asyncio.create_task(self.handle_input())
            output_task = asyncio.create_task(self.handle_output())

            # Wait for either task to complete or shutdown request
            done, pending = await asyncio.wait(
                [input_task, output_task], return_when=asyncio.FIRST_COMPLETED
            )

            # Cancel remaining tasks gracefully
            for task in pending:
                if not task.done():
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass

        except websockets.exceptions.ConnectionClosed:
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

    async def handle_input(self):
        """Read from stdin and send to WebSocket."""
        loop = asyncio.get_event_loop()

        while self.running and self.websocket and not self.shutdown_requested:
            try:
                # Read single character from stdin
                char = await loop.run_in_executor(None, sys.stdin.read, 1)
                if char:
                    # Send to terminal
                    await self.websocket.send(
                        json.dumps({"type": "input", "data": char})
                    )
                else:
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
                data = json.loads(message)

                if data.get("type") == "output":
                    # Write to stdout
                    sys.stdout.write(data.get("data", ""))
                    sys.stdout.flush()

            except websockets.exceptions.ConnectionClosed:
                print("\nTerminal session ended.")
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
        self.restore_terminal()

        # Close WebSocket connection gracefully
        if self.websocket:
            try:
                await self.websocket.close()
            except Exception as e:
                print(f"Warning: Error closing WebSocket: {e}")
