#!/usr/bin/env python3
"""
RaySSH - Ray-native terminal tool
WebSocket-based terminal communication using Ray actors.
"""

import asyncio
import argparse
import os
import signal
import subprocess
import sys
import json
from datetime import datetime

from terminal import RaySSHTerminal
from cli import (
    get_random_worker_node,
    get_node_by_index,
    print_nodes_table,
    interactive_node_selector,
    submit_file_job,
    submit_shell_command,
    handle_lab_command,
    handle_code_command,
)
from utils import ensure_ray_initialized, find_node_by_ip


def signal_handler(signum, frame):
    """Handle Ctrl+C gracefully."""
    print(f"\nReceived signal {signum}, initiating graceful shutdown...")
    # The RaySSHTerminal class has its own signal handlers
    # This is a fallback for any unhandled signals
    sys.exit(0)


def main():
    """Main entry point."""
    # Check if RAY_ADDRESS is set for remote mode
    ray_address_env = os.environ.get("RAY_ADDRESS")
    working_dir = None
    node_arg = None

    # Handle special commands first
    if len(sys.argv) == 1:
        # No arguments - connect to random worker node (local) or remote HOME
        if ray_address_env:
            working_dir = None  # No working_dir means HOME
        else:
            # Cluster connection - randomly connect to a worker node
            try:
                ensure_ray_initialized()
                # Try previous session first
                last_path = os.path.expanduser("~/.rayssh/last_session.json")
                prefer_ip = None
                try:
                    if os.path.isfile(last_path):
                        with open(last_path, "r", encoding="utf-8") as f:
                            data = json.load(f)
                            prefer_ip = data.get("node_ip")
                    if prefer_ip:
                        node_info = find_node_by_ip(prefer_ip)
                        if node_info and node_info.get("Alive"):
                            node_arg = prefer_ip
                            print(f"🔁 Reconnecting to previous node: {node_arg}")
                        else:
                            prefer_ip = None
                except Exception:
                    prefer_ip = None

                if not node_arg:
                    selected_node = get_random_worker_node()
                    node_arg = selected_node.get("NodeManagerAddress")
                    print(f"🎲 Connecting to worker node: {node_arg}")
            except ValueError as e:
                print(f"Error: {e}")
                return 1
            except Exception as e:
                print(f"Error selecting random worker node: {e}", file=sys.stderr)
                return 1

    elif len(sys.argv) == 2:
        argument = sys.argv[1]

        # Handle help command
        if argument in ["--help", "-h"]:
            print_help()
            return 0

        # Handle lab subcommand
        elif argument == "lab":
            return handle_lab_command(["lab"] + sys.argv[2:])

        # Handle code subcommand
        elif argument == "code":
            return handle_code_command(["code"] + sys.argv[2:])

        # Handle special commands
        elif argument in ["--ls"]:
            return print_nodes_table()
        elif argument in ["--list", "--show", "-l"]:
            selected_node_ip = interactive_node_selector()
            if selected_node_ip is None:
                print("\n❌ Cancelled.")
                return 0
            node_arg = selected_node_ip

        # Check if it's a file for job submission
        elif os.path.exists(argument) and os.path.isfile(argument) and "." in argument:
            # It's a file - submit as Ray job
            return submit_file_job(argument, no_wait=False)

        # Check if it's a directory and RAY_ADDRESS is set
        elif ray_address_env and os.path.exists(argument) and os.path.isdir(argument):
            # It's a directory and we're in remote mode - upload and connect
            working_dir = argument
            print(f"📁 Directory specified: {argument}")
            print(f"📦 Uploading directory: {os.path.abspath(working_dir)}")

        # Check if it's a directory but no RAY_ADDRESS
        elif os.path.exists(argument) and os.path.isdir(argument):
            print(
                "Error: Directory specified but RAY_ADDRESS not set.",
                file=sys.stderr,
            )
            print(
                "Set RAY_ADDRESS to enable remote mode with directory upload.",
                file=sys.stderr,
            )
            return 1

        # Handle node index argument (-0, -1, -2, etc.)
        elif argument.startswith("-") and argument[1:].isdigit():
            try:
                ensure_ray_initialized()
                index = int(argument[1:])  # Extract number after '-'
                node = get_node_by_index(index)
                # Use the node's IP address as the connection target
                node_arg = node.get("NodeManagerAddress")
                print(f"🔗 Connecting to node -{index}: {node_arg}")
            except ValueError as e:
                print(f"Error: {e}", file=sys.stderr)
                print("Use 'rayssh --ls' to see available nodes", file=sys.stderr)
                return 1
            except Exception as e:
                print(f"Error getting node by index: {e}", file=sys.stderr)
                return 1

        # Otherwise, treat as node argument
        else:
            node_arg = argument

    elif len(sys.argv) == 3:
        # Handle -q file pattern for quick job submission
        if sys.argv[1] == "-q":
            potential_file = sys.argv[2]
            if (
                os.path.exists(potential_file)
                and os.path.isfile(potential_file)
                and "." in potential_file
            ):
                return submit_file_job(potential_file, no_wait=True)
            else:
                print(
                    f"Error: File '{potential_file}' not found or not a valid file",
                    file=sys.stderr,
                )
                return 1

        # Handle lab and code subcommands with a single extra argument
        elif sys.argv[1] == "lab":
            return handle_lab_command(["lab"] + sys.argv[2:])
        elif sys.argv[1] == "code":
            return handle_code_command(["code"] + sys.argv[2:])
        # Handle -- <command>
        elif sys.argv[1] == "--":
            import shlex
            return submit_shell_command(shlex.join(sys.argv[2:]))

        # Handle -0 lab and -0 code patterns
        elif sys.argv[1] == "-0":
            if sys.argv[2] == "lab":
                return handle_lab_command(["-0", "lab"] + sys.argv[3:])
            elif sys.argv[2] == "code":
                return handle_code_command(["-0", "code"] + sys.argv[3:])
            else:
                # Fall through to node index handling
                pass

    elif len(sys.argv) > 3:
        # Handle lab and code commands with additional arguments (like paths)
        if sys.argv[1] == "lab":
            return handle_lab_command(["lab"] + sys.argv[2:])
        elif sys.argv[1] == "code":
            return handle_code_command(["code"] + sys.argv[2:])
        elif sys.argv[1] == "--":
            # Join the rest as a properly quoted command string
            import shlex
            return submit_shell_command(shlex.join(sys.argv[2:]))
        elif sys.argv[1] == "-0" and len(sys.argv) >= 4:
            if sys.argv[2] == "lab":
                return handle_lab_command(["-0", "lab"] + sys.argv[3:])
            elif sys.argv[2] == "code":
                return handle_code_command(["-0", "code"] + sys.argv[3:])
            else:
                print_help()
                return 1
        else:
            # More than 2 arguments - for now, just show help
            print_help()
            return 1

    # Initialize Ray with working directory if specified
    if ray_address_env and working_dir is not None:
        try:
            ensure_ray_initialized(ray_address=ray_address_env, working_dir=working_dir)
        except Exception as e:
            print(f"Error initializing Ray: {e}", file=sys.stderr)
            return 1
    elif node_arg:
        # Cluster connection - ensure Ray is initialized
        try:
            import ray as _ray

            if not _ray.is_initialized():
                ensure_ray_initialized()
        except Exception as e:
            print(f"Error initializing Ray: {e}", file=sys.stderr)
            return 1

    # Set up fallback signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Create and run terminal client
    if ray_address_env and working_dir is not None:
        # Ray client mode with working directory - use RaySSHTerminal in remote mode
        terminal = RaySSHTerminal(
            None, ray_address=ray_address_env, working_dir=working_dir
        )
    elif ray_address_env and node_arg is None:
        # Ray client mode without working directory and no specific node - use RaySSHTerminal in remote mode
        terminal = RaySSHTerminal(None, ray_address=ray_address_env, working_dir=None)
    else:
        # Cluster connection - connect to specific node (even with RAY_ADDRESS set)
        terminal = RaySSHTerminal(node_arg, ray_address=ray_address_env)

    # Persist last session info for cluster node connections
    try:
        if node_arg and not ray_address_env:
            os.makedirs(os.path.expanduser("~/.rayssh"), exist_ok=True)
            last_path = os.path.expanduser("~/.rayssh/last_session.json")
            payload = {"node_ip": node_arg, "timestamp": datetime.now().isoformat(timespec="seconds")}
            with open(last_path, "w", encoding="utf-8") as f:
                json.dump(payload, f)
    except Exception:
        pass

    try:
        asyncio.run(terminal.run())
    except KeyboardInterrupt:
        print("\nSession interrupted by user.")
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)
    finally:
        print("👋 Goodbye!")


def print_help():
    """Print help information."""
    help_text = """
RaySSH: Ray-native terminal tool

Usage:
    rayssh                          # Randomly connect to a worker node at remote HOME
    rayssh <ip|node_id|-index>      # Connect to specific node
    rayssh <dir>                    # Remote mode with directory upload (requires RAY_ADDRESS)
    rayssh [-q] <file>              # Submit file as Ray job, -q for no-wait
    rayssh -l                       # Interactive node selection
    rayssh --ls                     # Print nodes table
    rayssh [lab|code] [path]        # Launch Jupyter Lab / code-server on remote
    rayssh -- <command>             # Submit shell command as job

Options:
    -h, --help                      # Show help
    -l, --list, --show              # Interactive node selection
    --ls                            # Print nodes table
    -q                              # Quick mode (no-wait for jobs)
    -- <command>                    # Submit shell command as job

Examples:
    rayssh                          # Random worker node
    rayssh 192.168.1.100            # Connect by IP
    rayssh -1                       # Connect to first worker
    rayssh -l                       # Interactive node selection
    rayssh --ls                     # Show nodes table
    rayssh ./myproject              # Upload and work in directory (remote mode)
    rayssh [-q] script.py           # Submit Python job and wait. "-q" for no-wait.
    rayssh lab                      # Launch Jupyter Lab on worker node
    rayssh code ./src               # Launch code-server with uploaded directory
    rayssh -- nvidia-smi            # Submit shell command as job and tail logs
    n_gpus=8 rayssh train.py        # GPUs to request for job (through --entrypoint-num-gpus)

Environment Variables:
    RAY_ADDRESS=ray://host:port     # Enable remote mode

🖥️  Terminal features: Real-time shell via WebSockets, graceful shutdown
🌐 Remote mode: Upload local directories, work on remote clusters  
🚀 Job submission: Python/Bash files, with working dir upload.
🔬 Lab features: Jupyter Lab on Ray nodes with optional working dir upload
💻 Code features: VS Code server on Ray nodes with working dir upload
"""
    print(help_text.strip())


if __name__ == "__main__":
    main()
