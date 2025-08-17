#!/usr/bin/env python3
"""
RaySSH terminal CLI interface.
"""

import sys
import signal
import asyncio
import ray

from .server import TerminalActor
from .ws_client import TerminalClient
from utils import ensure_ray_initialized, find_target_node


class RaySSHTerminal:
    """Main RaySSH client using terminal actors."""

    def __init__(
        self, node_arg: str = None, ray_address: str = None, working_dir: str = None
    ):
        self.node_arg = node_arg
        self.ray_address = ray_address
        self.working_dir = working_dir
        self.terminal_actor = None
        self.target_node = None
        self.client = None
        self.shutdown_requested = False
        self.is_remote_mode = ray_address is not None
        self._setup_signal_handlers()

    def _setup_signal_handlers(self):
        """Set up signal handlers for graceful shutdown."""

        def signal_handler(signum, frame):
            print(f"\nReceived signal {signum}, initiating graceful shutdown...")
            self.shutdown_requested = True

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    def initialize_ray(self):
        """Initialize Ray connection."""
        if self.is_remote_mode:
            ensure_ray_initialized(
                ray_address=self.ray_address, working_dir=self.working_dir
            )
            # For remote mode, we don't need to find a specific target node
            # The terminal actor will be deployed remotely
            self.target_node = {"NodeManagerAddress": "remote", "NodeName": "remote"}
        else:
            print("Initializing Ray...")
            ensure_ray_initialized()

            # Find target node for local mode
            self.target_node = find_target_node(self.node_arg)
            if not self.target_node:
                print(f"Could not find target node: {self.node_arg}")
                sys.exit(1)

            print(
                f"Target node: {self.target_node['NodeName']} ({self.target_node['NodeManagerAddress']})"
            )

    def start_terminal_actor(self):
        """Start the terminal actor on target node."""
        print("🌐 Deploying terminal actor...")

        # Create terminal actor on target node with working directory if specified
        if self.is_remote_mode and self.working_dir:
            # For remote mode with working directory, pass it to the actor
            self.terminal_actor = TerminalActor.remote(working_dir=self.working_dir)
        else:
            # For local mode or remote mode without working directory
            self.terminal_actor = TerminalActor.remote()

        # Start the terminal server
        server_info = ray.get(self.terminal_actor.start_terminal_server.remote())
        return server_info

    async def run(self):
        """Run the terminal session."""
        try:
            # Initialize Ray and find target
            self.initialize_ray()

            # Check for shutdown request
            if self.shutdown_requested:
                print("Shutdown requested during initialization")
                return

            # Start terminal actor
            server_info = self.start_terminal_actor()

            if not server_info:
                print("Failed to start terminal server")
                return

            # Check for shutdown request
            if self.shutdown_requested:
                print("Shutdown requested before connection")
                return

            # Connect to terminal
            self.client = TerminalClient()
            # In remote mode, use the actual IP from server_info instead of "remote"
            if self.is_remote_mode:
                connection_host = server_info["ip"]
            else:
                connection_host = self.target_node["NodeManagerAddress"]

            await self.client.connect_to_terminal(connection_host, server_info["port"])

        except KeyboardInterrupt:
            print("\nKeyboard interrupt received, shutting down gracefully...")
        except Exception as e:
            print(f"Error: {e}")
        finally:
            await self.cleanup()

    async def cleanup(self):
        """Clean up resources gracefully."""
        # Close WebSocket client first
        if self.client:
            try:
                await self.client.cleanup()
                # Ensure prompt moves to new line after raw mode restore
                try:
                    sys.stdout.write("\n")
                    sys.stdout.flush()
                except Exception:
                    pass
                print("✓ WebSocket client closed")
            except Exception as e:
                print(f"Warning: Error closing client: {e}")

        # Stop terminal server
        if self.terminal_actor:
            try:
                ray.get(self.terminal_actor.stop_terminal_server.remote())
                print("✓ Terminal server stopped")
            except Exception as e:
                print(f"Warning: Error stopping server: {e}")

        # Print final cleanup message on a clean line
        print("Press Enter to confirm exit...")
