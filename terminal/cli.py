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

from .server import TerminalActor, WorkdirActor, GPUDaemonActor
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

    def _create_gpu_daemon_actor(self, n_gpus: float, session_id: str, preferred_node_id: str = None):
        """Create GPU daemon actor with soft node affinity to preferred node."""
        try:
            # print(f"🎛️ Creating GPU daemon actor for {n_gpus} GPUs")
            
            # Use soft scheduling strategy - prefer previous session's node but allow fallback
            if preferred_node_id:
                scheduling_strategy = ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
                    node_id=preferred_node_id, soft=True  # Soft affinity allows fallback
                )
                print(f"🎛️ Asking for {n_gpus} GPUs (last-connected node first)")
            else:
                # No preferred node, let Ray choose any node with GPUs
                # we use PACK to try to reduce "gpu fragmentation" on a cluster-level
                scheduling_strategy = "PACK"  # Ray's default GPU placement
            
            gpu_daemon_actor = GPUDaemonActor.options(
                num_gpus=n_gpus,
                scheduling_strategy=scheduling_strategy
            ).remote(session_id)
            
            # Get GPU environment from daemon actor
            gpu_env = ray.get(gpu_daemon_actor.get_gpu_env.remote())
            cuda_visible_devices = gpu_env.get("CUDA_VISIBLE_DEVICES", "")
            
            # print(f"✅ GPU daemon created: CUDA_VISIBLE_DEVICES={cuda_visible_devices}")
            
            return gpu_daemon_actor, cuda_visible_devices
            
        except Exception as e:
            print(f"❌ Failed to create GPU daemon actor: {e}")
            return None, None

    def initialize_ray(self):
        """Initialize Ray connection."""
        # Always use RAY_ADDRESS if set to ensure proper module shipping
        ensure_ray_initialized(
            ray_address=self.ray_address,
            working_dir=self.working_dir if self.is_remote_mode else None,
        )

        if self.is_remote_mode:
            # Ray Client mode: prefer last-connected node (soft), fallback to selection
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
        # Terminal actor always uses 0 GPUs (GPU allocation handled separately by client)
        actor_options = {"num_gpus": 0}

        # We always have a specific target node now, so always use NodeAffinitySchedulingStrategy
        if not self.target_node or not self.target_node.get("NodeID"):
            print("❌ No target node available for actor placement")
            return None

        target_node_id = self.target_node["NodeID"]
        target_ip = self.target_node["NodeManagerAddress"]

        # Use soft placement: prefer chosen node, but allow Ray to place elsewhere if needed
        actor_options["scheduling_strategy"] = (
            ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
                node_id=target_node_id, soft=True
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
                self.terminal_actor.start_terminal_server.remote(), timeout=10.0
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
            # Initialize Ray and find initial target
            self.initialize_ray()

            # Check for shutdown request
            if self.shutdown_requested:
                print("Shutdown requested during initialization")
                return

            # Step 1: Handle GPU allocation first (if needed) to determine final target node
            n_gpus = parse_n_gpus_from_env()
            cuda_visible_devices = None
            gpu_daemon_actor = None
            
            if n_gpus is not None and n_gpus > 0:
                # Generate session ID for GPU daemon
                import uuid
                session_id = uuid.uuid4().hex
                
                # Try to prefer the target node for GPU allocation (soft affinity)
                preferred_node_id = self.target_node.get("NodeID") if self.target_node else None
                
                # Create GPU daemon actor on client side
                gpu_daemon_actor, cuda_visible_devices = self._create_gpu_daemon_actor(
                    n_gpus, session_id, preferred_node_id
                )
                
                if gpu_daemon_actor and cuda_visible_devices:
                    # Get the actual node where GPU daemon was placed
                    try:
                        # Single call to get both node ID and IP from GPU daemon
                        gpu_node_info = ray.get(gpu_daemon_actor.get_node_info.remote())
                        
                        if gpu_node_info:
                            # Update target node with GPU node info
                            self.target_node = {
                                "NodeID": gpu_node_info["node_id"],
                                "NodeManagerAddress": gpu_node_info["node_ip"],
                                "Alive": True
                            }
                            # print(f"🎯 Using GPU node: {gpu_node_info['node_ip']}")
                        
                    except Exception as e:
                        print(f"⚠️  Could not determine GPU node location: {e}")

            # Step 2: Resolve working directory for the final target node
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

            # Step 3: Start terminal actor on the final target node (only once!)
            server_info = self.start_terminal_actor()

            if not server_info:
                print("Failed to start terminal server")
                if gpu_daemon_actor:
                    ray.kill(gpu_daemon_actor)
                return

            # Check for shutdown request
            if self.shutdown_requested:
                print("Shutdown requested before connection")
                return

            # Step 4: Connect to terminal
            self.client = TerminalClient()
            connection_host = server_info["ip"]
            
            await self.client.connect_to_terminal(
                connection_host, 
                server_info["port"], 
                working_dir=session_workdir, 
                cuda_visible_devices=cuda_visible_devices,
                gpu_daemon_actor=gpu_daemon_actor
            )

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
