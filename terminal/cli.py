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
from utils import ensure_ray_initialized, find_target_node, parse_n_gpus_from_env


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

            # Find target node for cluster connection
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

        # Parse GPU requirements from environment
        n_gpus = parse_n_gpus_from_env()
        if n_gpus is not None:
            print(f"🎛️ GPUs requested: {n_gpus}")
            actor_options = {"num_gpus": n_gpus}
        else:
            actor_options = {}

        # Configure scheduling strategy based on mode and target node
        if self.is_remote_mode:
            # Ray Client mode: use SPREAD scheduling (head has 0 CPUs)
            actor_options["scheduling_strategy"] = "SPREAD"
        else:
            # Cluster connection: if user specified a target node, place actor there specifically
            if self.target_node and self.target_node.get("NodeID"):
                target_node_id = self.target_node["NodeID"]
                target_ip = self.target_node["NodeManagerAddress"]
                print(f"🎯 Targeting specific node: {target_ip} (ID: {target_node_id[:8]}...)")
                actor_options["scheduling_strategy"] = ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
                    node_id=target_node_id, soft=False
                )
            else:
                # No specific target, use SPREAD
                actor_options["scheduling_strategy"] = "SPREAD"

        # Create terminal actor
        try:
            if self.working_dir:
                self.terminal_actor = TerminalActor.options(**actor_options).remote(working_dir=self.working_dir)
            else:
                self.terminal_actor = TerminalActor.options(**actor_options).remote()
        except Exception as e:
            if not self.is_remote_mode and self.target_node:
                print(f"❌ Failed to place actor on target node {self.target_node['NodeManagerAddress']}: {e}")
                print("   The node may be busy, out of resources, or have conflicting actors.")
                return None
            else:
                raise

        # Start the terminal server (interruptible wait)
        start_ref = self.terminal_actor.start_terminal_server.remote()
        while True:
            if self.shutdown_requested:
                print("🛑 Deployment interrupted. Cleaning up...")
                try:
                    ray.get(
                        self.terminal_actor.stop_terminal_server.remote(), timeout=5.0
                    )
                except Exception:
                    pass
                try:
                    ray.kill(self.terminal_actor)
                except Exception:
                    pass
                return None
            try:
                return ray.get(start_ref, timeout=1.0)
            except Exception as e:
                # Continue polling on timeout; re-raise other errors
                from ray.exceptions import GetTimeoutError

                if isinstance(e, GetTimeoutError):
                    continue
                raise

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
                print("Cancelled starting terminal server")
                return

            # Check for shutdown request
            if self.shutdown_requested:
                print("Shutdown requested before connection")
                return

            # Connect to terminal
            self.client = TerminalClient()
            # Always use the actual IP from server_info (where the actor is actually running)
            connection_host = server_info["ip"]
            
            # Check for IP mismatch and warn user BEFORE connecting
            if self.target_node:
                requested_ip = self.target_node["NodeManagerAddress"]
                if connection_host != requested_ip:
                    print(f"⚠️  WARNING: You requested {requested_ip} but actor was placed on {connection_host}")
                    print(f"   This may happen if the target node is busy or unavailable.")

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
