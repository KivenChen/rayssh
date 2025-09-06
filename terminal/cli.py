#!/usr/bin/env python3
"""
RaySSH terminal CLI interface.
"""

import sys
import signal
import asyncio
import time
import ray
import os

from .server import TerminalActor, WorkdirActor
from .ws_client import TerminalClient
from utils import (
    ensure_ray_initialized,
    find_target_node,
    parse_n_gpus_from_env,
    write_last_session_node_ip,
    select_worker_node,
)


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
        # Remote mode only when ray_address is set AND no specific node is requested
        self.is_remote_mode = ray_address is not None and node_arg is None
        self._setup_signal_handlers()

    def _setup_signal_handlers(self):
        """Set up signal handlers for graceful shutdown."""

        def signal_handler(signum, frame):
            print(f"\nReceived signal {signum}, initiating graceful shutdown...")
            self.shutdown_requested = True

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    def _get_ray_working_dir_gcs_uri(self):
        """Get the GCS URI for the uploaded working directory from Ray runtime context."""
        try:
            runtime_context = ray.get_runtime_context()
            runtime_env = runtime_context.runtime_env
            working_dir_gcs_uri = runtime_env.get('working_dir')
            
            if working_dir_gcs_uri and working_dir_gcs_uri.startswith('gcs://'):
                return working_dir_gcs_uri
            else:
                return None
        except Exception:
            return None

    def initialize_ray(self):
        """Initialize Ray connection."""
        # Always use RAY_ADDRESS if set to ensure proper module shipping
        ensure_ray_initialized(
            ray_address=self.ray_address,
            working_dir=self.working_dir if self.is_remote_mode else None,
        )

        if self.is_remote_mode:
            # Ray Client mode: select a worker node ourselves instead of using SPREAD
            n_gpus = parse_n_gpus_from_env()
            try:
                self.target_node = select_worker_node(n_gpus=n_gpus)
            except RuntimeError as e:
                print(f"❌ {e}")
                sys.exit(1)
        else:
            # Cluster connection: find the specific target node
            self.target_node = find_target_node(self.node_arg)
            if not self.target_node:
                print(f"Could not find target node: {self.node_arg}")
                sys.exit(1)

    def start_terminal_actor(self):
        """Start the terminal actor on target node."""
        # Parse GPU requirements from environment
        n_gpus = parse_n_gpus_from_env()
        if n_gpus is not None:
            print(f"🎛️ GPUs requested: {n_gpus}")
            actor_options = {"num_gpus": n_gpus}
        else:
            actor_options = {}

        # We always have a specific target node now, so always use NodeAffinitySchedulingStrategy
        if not self.target_node or not self.target_node.get("NodeID"):
            print("❌ No target node available for actor placement")
            return None

        target_node_id = self.target_node["NodeID"]
        target_ip = self.target_node["NodeManagerAddress"]

        actor_options["scheduling_strategy"] = (
            ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
                node_id=target_node_id, soft=False
            )
        )

        # Use one persistent actor per node to share port 8080
        safe_ip = str(target_ip).replace(".", "-")
        actor_name = f"rayssh_terminal_{safe_ip}"

        # Try to connect to existing server first (avoid ray.get() calls)
        try:
            existing_actor = ray.get_actor(actor_name, namespace="rayssh")
            # Test if server is running by trying to connect to port 8080
            import socket

            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2)

            try:
                result = sock.connect_ex((target_ip, 8080))
                if result == 0:
                    sock.close()
                    # Server is running, we can connect to it
                    self.terminal_actor = existing_actor
                    return {"ip": target_ip, "port": 8080}
                else:
                    sock.close()
                    # Server not running, try to start it
                    self.terminal_actor = existing_actor
            except Exception:
                sock.close()
                # Server not running, try to start it
                self.terminal_actor = existing_actor

        except Exception:
            # No existing actor, create new one
            try:
                actor_options.update(
                    {"name": actor_name, "lifetime": "detached", "namespace": "rayssh"}
                )

                print(f"🌐 Deploying terminal actor to remote")

                # Create actor with default working_dir (will be overridden per session)
                self.terminal_actor = TerminalActor.options(**actor_options).remote(
                    working_dir=self.working_dir
                )

            except Exception as e:
                print(f"❌ Failed to place actor on target node {target_ip}: {e}")
                return None

        # Start the terminal server (single call with timeout)
        try:
            server_info = ray.get(
                self.terminal_actor.start_terminal_server.remote(), timeout=5.0
            )

            if server_info:
                return server_info
            else:
                return None

        except Exception as e:
            if self.shutdown_requested:
                try:
                    ray.kill(self.terminal_actor)
                except Exception:
                    pass
                return None
            else:
                print(f"Server startup error: {e}")
                return None

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
            # Always use the actual IP from server_info (where the actor is actually running)
            connection_host = server_info["ip"]

            # Check for IP mismatch and warn user
            if self.target_node:
                requested_ip = self.target_node["NodeManagerAddress"]
                if connection_host != requested_ip:
                    print(
                        f"⚠️  Connecting to {connection_host} instead of requested {requested_ip}"
                    )

            # Determine working directory for the session
            session_workdir = None
            if self.working_dir:
                if self.is_remote_mode:
                    # In remote mode, get the Ray-managed working directory path
                    try:
                        # Get the GCS URI from Ray runtime context
                        working_dir_gcs_uri = self._get_ray_working_dir_gcs_uri()
                        
                        if working_dir_gcs_uri:
                            # Create WorkdirActor with GCS URI to resolve Ray-managed working directory
                            workdir_actor = WorkdirActor.options(
                                runtime_env={"working_dir": working_dir_gcs_uri},
                                scheduling_strategy=ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
                                    node_id=self.target_node["NodeID"], soft=False
                                )
                            ).remote()
                            
                            session_workdir = ray.get(workdir_actor.get_working_dir.remote())
                            
                            # Clean up the workdir actor
                            # ray.kill(workdir_actor)
                        else:
                            print(f"⚠️  No GCS working directory URI found, using local path")
                            session_workdir = os.path.abspath(self.working_dir)
                        
                    except Exception as e:
                        print(f"⚠️  Error resolving Ray working directory: {e}, using local path")
                        session_workdir = os.path.abspath(self.working_dir)
                else:
                    # In local mode, use the absolute path
                    session_workdir = os.path.abspath(self.working_dir)
            
            await self.client.connect_to_terminal(connection_host, server_info["port"], working_dir=session_workdir)

            # Save the successfully connected IP as last session
            write_last_session_node_ip(connection_host)

            # Save the successfully connected IP as last session
            write_last_session_node_ip(connection_host)

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

        # Keep the server running for future connections (don't stop it)
        if self.terminal_actor:
            print("✓ Session ended (server remains active)")

        print("Press Enter to confirm exit...")
