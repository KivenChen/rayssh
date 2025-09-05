#!/usr/bin/env python3
"""
Cursor integration utilities for RaySSH CLI.
"""

import os
import sys
from typing import List


def handle_tell_cursor_command(argv: List[str]) -> int:
    """
    Handle the 'tell-cursor' command that creates a .cursorrules file.
    
    Args:
        argv: Command line arguments
        
    Returns:
        Exit code (0 for success, 1 for error)
    """
    # Parse arguments - expect ['tell-cursor'] or ['tell-cursor', 'path']
    args = argv
    if len(argv) >= 1 and argv[0] == "tell-cursor":
        args = argv[1:]
    
    # Determine target directory (current directory by default)
    target_dir = "."
    if len(args) >= 1:
        target_dir = args[0]
        if not os.path.exists(target_dir):
            print(f"Error: Directory '{target_dir}' does not exist", file=sys.stderr)
            return 1
        if not os.path.isdir(target_dir):
            print(f"Error: '{target_dir}' is not a directory", file=sys.stderr)
            return 1
    
    # Create the .cursorrules file content
    cursorrules_content = '''# 🚀 RaySSH Debug Rules for Cursor

## Debugging This Project

**IMPORTANT**: To properly debug this project, you MUST first establish a remote terminal connection using:

```bash
rayssh .
```

This command will:
- Connect you to a Ray worker node
- Upload the current directory to the remote environment  
- Give you a working terminal on the remote cluster
- Allow you to run and debug code in the actual Ray environment

## Why This Matters

This project uses Ray for distributed computing. Simply running code locally won't work because:
- Ray actors and tasks need to run on the cluster
- Dependencies and environment setup happen on remote nodes
- File paths and working directories are different in the cluster context

## Debugging Workflow

1. **First, always run**: `rayssh .`
2. **Then you can**: 
   - Run Python scripts: `python your_script.py`
   - Check Ray status: `ray status`
   - Debug interactively: `python -i your_script.py`
   - Install packages: `pip install package_name`
   - Edit files: `vim/nano file.py`

## Other Useful RaySSH Commands

- `rayssh --ls` - List available nodes
- `rayssh lab` - Launch Jupyter Lab on remote
- `rayssh code` - Launch VS Code server on remote
- `rayssh -l` - Interactive node selection

## Remember

🎯 **Always start with `rayssh .` before attempting to debug!**

Without this, you'll be debugging in the wrong environment and things won't work as expected.
'''

    # Write the .cursorrules file
    cursorrules_path = os.path.join(target_dir, ".cursorrules")
    
    try:
        with open(cursorrules_path, "w", encoding="utf-8") as f:
            f.write(cursorrules_content)
        
        abs_path = os.path.abspath(cursorrules_path)
        print("🎯 Cursor has been informed!")
        print(f"📝 Created .cursorrules file: {abs_path}")
        print()
        print("✨ Now Cursor knows that to debug this project, it should run:")
        print("   💫 rayssh .")
        print()
        print("🤖 Your AI assistant will be much smarter about Ray debugging!")
        
        return 0
        
    except Exception as e:
        print(f"Error creating .cursorrules file: {e}", file=sys.stderr)
        return 1 