#!/usr/bin/env python3
"""
Sync command handler for RaySSH CLI.
Uploads a directory and starts a terminal with sync enabled.
"""

import os
import sys
from typing import List, Optional

from terminal import RaySSHTerminal
from utils import ensure_ray_initialized


def handle_sync_command(argv: List[str]) -> int:
    """Handle the sync command - upload directory and start terminal with sync enabled"""
    args = argv
    if len(argv) >= 1 and argv[0] == "sync":
        args = argv[1:]

    # Parse arguments
    if len(args) == 0:
        print("Error: sync command requires a directory argument", file=sys.stderr)
        print("Usage: rayssh sync <directory>", file=sys.stderr)
        return 1

    sync_dir = args[0]
    node_arg = None

    # Handle optional node argument
    if len(args) >= 2:
        node_arg = args[1]

    # Validate directory
    if not os.path.exists(sync_dir):
        print(f"Error: Directory '{sync_dir}' does not exist", file=sys.stderr)
        return 1

    if not os.path.isdir(sync_dir):
        print(f"Error: '{sync_dir}' is not a directory", file=sys.stderr)
        return 1

    # Get absolute path
    sync_dir = os.path.abspath(sync_dir)

    print(f"📁 Textfile changes will be uploaded from {sync_dir}")

    # Check RAY_ADDRESS for remote mode
    ray_address_env = os.environ.get("RAY_ADDRESS")

    try:
        # Initialize Ray with the directory as working_dir (uploads it)
        ensure_ray_initialized(ray_address=ray_address_env, working_dir=sync_dir)
    except Exception as e:
        print(f"Error initializing Ray with directory: {e}", file=sys.stderr)
        return 1

    # Get require constraints from environment
    from utils import parse_require_constraints_from_env

    require_constraints = parse_require_constraints_from_env()

    # Create terminal with sync enabled
    terminal = RaySSHTerminal(
        node_arg,
        ray_address=ray_address_env,
        working_dir=sync_dir,
        enable_sync=True,
        sync_local_root=sync_dir,
        resource_constraints=require_constraints,
    )

    try:
        import asyncio

        asyncio.run(terminal.run())
    except KeyboardInterrupt:
        print("\nSync session interrupted by user.")
    except Exception as e:
        print(f"Fatal error in sync session: {e}")
        return 1
    finally:
        print("👋 Sync session ended!")

    return 0
