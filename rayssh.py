#!/usr/bin/env python3

import atexit
import os
import queue
import signal
import subprocess
import sys
import tempfile
import termios
import threading
import time
import tty

import ray

# Try to import readline for tab completion
try:
    import readline
    READLINE_AVAILABLE = True
except ImportError:
    READLINE_AVAILABLE = False

import re

from shell_actor import ShellActor
from utils import (
    ensure_ray_initialized,
    find_target_node,
    get_node_resources,
    get_ray_cluster_nodes,
)


class RaySSHClient:
    """Main client for RaySSH that provides an interactive shell interface."""

    def __init__(self, node_arg: str):
        """Initialize the RaySSH client for the target node."""
        self.node_arg = node_arg
        self.shell_actor = None
        self.target_node = None
        self.current_command_future = None
        self.current_interactive_session = None  # Track active interactive session
        self.shutdown_event = threading.Event()

        # Set up signal handlers
        signal.signal(signal.SIGINT, self._handle_sigint)
        signal.signal(signal.SIGTERM, self._handle_sigterm)

        # Register cleanup function
        atexit.register(self.cleanup)

    def _handle_sigint(self, signum, frame):
        """Handle Ctrl-C (SIGINT)."""
        if self.current_command_future and not self.current_command_future.done():
            # Interrupt the currently running command on the remote shell
            try:
                ray.get(self.shell_actor.interrupt_current_command.remote())
                print("\n^C")
            except Exception as e:
                print(f"\nWarning: Could not interrupt command: {e}")
                # Force cancel the future to prevent hanging
                try:
                    self.current_command_future.cancel()
                except Exception:
                    pass
        elif self.current_interactive_session:
            # Interrupt the interactive session instead of exiting shell
            try:
                ray.get(self.shell_actor.terminate_interactive_command.remote(self.current_interactive_session))
                print("\n^C")
                self.current_interactive_session = None
            except Exception:
                pass
        else:
            # No command running, exit the shell
            print("\nExiting...")
            self.shutdown_event.set()

    def _handle_sigterm(self, signum, frame):
        """Handle SIGTERM."""
        print("\nTerminating...")
        self.shutdown_event.set()

    def initialize(self):
        """Initialize Ray and set up the shell actor on the target node."""
        print("Initializing RaySSH...")

        # Ensure Ray is initialized
        ensure_ray_initialized()

        # Find the target node
        try:
            self.target_node = find_target_node(self.node_arg)
            node_resources = get_node_resources(self.target_node)
            print(f"Found target node: {node_resources['node_ip']} (ID: {node_resources['node_id'][:8]}...)")
        except ValueError as e:
            print(f"Error: {e}", file=sys.stderr)
            return False

        # Deploy shell actor on the target node
        try:
            # Use scheduling hints to place the actor on the specific node
            self.shell_actor = ShellActor.options(
                scheduling_strategy=ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
                    node_id=self.target_node['NodeID'],
                    soft=False
                )
            ).remote()

            # Verify the actor is running and get node info
            node_info = ray.get(self.shell_actor.get_node_info.remote())
            print(f"Shell actor deployed on {node_info['hostname']} (user: {node_info['user']})")
            print(f"Initial working directory: {node_info['cwd']}")
            print()

        except Exception as e:
            print(f"Error deploying shell actor: {e}", file=sys.stderr)
            return False

        return True

    def get_prompt(self) -> str:
        """Generate a shell prompt."""
        try:
            node_info = ray.get(self.shell_actor.get_node_info.remote())
            hostname = node_info['hostname']
            user = node_info['user']  # Get the real user for home directory logic
            cwd = node_info['cwd']

            # Shorten home directory path using the real username
            home = f"/home/{user}"
            if cwd.startswith(home):
                if cwd == home:
                    # Exactly in home directory
                    display_cwd = "~"
                else:
                    # In a subdirectory of home, show only the basename
                    relative_path = cwd[len(home):].lstrip('/')
                    if '/' in relative_path:
                        display_cwd = os.path.basename(relative_path)
                    else:
                        display_cwd = relative_path
            else:
                # Not in home directory, show only the basename
                display_cwd = os.path.basename(cwd) if cwd != '/' else '/'

            # Always use "RaySSH" as the displayed username in the prompt
            return f"RaySSH@{hostname}:{display_cwd}$ "
        except Exception:
            return "rayssh$ "

    def _setup_tab_completion(self):
        """Set up tab completion for file names."""
        if not READLINE_AVAILABLE:
            return

        def complete_filenames(text, state):
            """Complete file names in the current directory."""
            try:
                # Get current working directory from the remote shell
                current_dir = ray.get(self.shell_actor.get_current_directory.remote())

                # Handle relative paths
                if text.startswith('/'):
                    # Absolute path
                    search_dir = os.path.dirname(text) or '/'
                    search_pattern = os.path.basename(text)
                elif '/' in text:
                    # Relative path with directories
                    search_dir = os.path.join(current_dir, os.path.dirname(text))
                    search_pattern = os.path.basename(text)
                else:
                    # Just a filename
                    search_dir = current_dir
                    search_pattern = text

                # Get file listings from remote shell
                list_command = f"ls -1a '{search_dir}' 2>/dev/null"
                result = ray.get(self.shell_actor.execute_command.remote(list_command))

                if result['returncode'] != 0:
                    return None

                # Parse the file list
                files = result['stdout'].strip().split('\n') if result['stdout'].strip() else []

                # Filter files that match the pattern
                matches = []
                for file in files:
                    if file and file.startswith(search_pattern):
                        if '/' in text:
                            # Reconstruct the full path for relative paths
                            if text.startswith('/'):
                                full_path = os.path.join(os.path.dirname(text), file)
                            else:
                                full_path = os.path.join(os.path.dirname(text), file)
                            matches.append(full_path)
                        else:
                            matches.append(file)

                # Return the appropriate match
                try:
                    return matches[state]
                except IndexError:
                    return None

            except Exception:
                return None

        try:
            # Set up readline
            readline.set_completer(complete_filenames)
            readline.parse_and_bind("tab: complete")
            # Set word delimiters to include common shell characters
            readline.set_completer_delims(' \t\n;|&()<>')
        except Exception:
            # If readline setup fails, continue without tab completion
            pass

    def _cleanup_tab_completion(self):
        """Clean up tab completion."""
        if READLINE_AVAILABLE:
            try:
                readline.set_completer(None)
            except Exception:
                pass

    def _filter_raylet_warnings(self, text: str) -> str:
        """Filter out noisy raylet warnings from output."""
        if not text:
            return text

        # Pattern to match raylet file system monitor warnings
        raylet_warning_pattern = r'\(raylet\) \[.*?\] \(raylet\) file_system_monitor\.cc.*?Object creation will fail if spilling is required\.\s*'

        # Remove raylet warnings
        filtered_text = re.sub(raylet_warning_pattern, '', text, flags=re.MULTILINE | re.DOTALL)

        return filtered_text

    def _handle_vim_command(self, command: str) -> bool:
        """
        Handle vim command by transferring file, opening vim locally, then syncing back.

        Returns:
            True to continue, False to exit
        """
        # Parse vim command to extract filename
        parts = command.strip().split()
        if len(parts) < 2:
            print("vim: missing filename", file=sys.stderr)
            return True

        filename = parts[1]

        try:
            # Read file from remote
            print(f"RaySSH: 📥 Transferring file from remote: {filename}")
            file_result = ray.get(self.shell_actor.read_file.remote(filename))

            if not file_result['success']:
                # File doesn't exist, create empty content for new file
                if "File not found" in file_result['error']:
                    content = ""
                    print(f"RaySSH: ✨ Creating new file: {filename}")
                else:
                    print(f"Error: {file_result['error']}", file=sys.stderr)
                    return True
            else:
                content = file_result['content']
                size_kb = file_result['size'] / 1024
                print(f"RaySSH: ✅ File transferred ({size_kb:.1f} KB)")

            # Create temporary file with descriptive name indicating remote source
            file_extension = os.path.splitext(filename)[1]
            base_name = os.path.basename(filename)
            
            # Get node info for temp file naming
            try:
                node_info = ray.get(self.shell_actor.get_node_info.remote())
                hostname = node_info['hostname']
                # Sanitize hostname for filename use
                safe_hostname = re.sub(r'[^\w\-.]', '_', hostname)
            except Exception:
                safe_hostname = "remote"
            
            # Create temp file with pattern: rayssh_HOSTNAME_FILENAME_XXXXXX.ext
            temp_prefix = f"rayssh_{safe_hostname}_{base_name}_"
            with tempfile.NamedTemporaryFile(mode='w', prefix=temp_prefix, suffix=file_extension, delete=False, encoding='utf-8') as temp_file:
                temp_file.write(content)
                temp_path = temp_file.name

            try:
                # Get original modification time
                original_mtime = os.path.getmtime(temp_path)

                # Launch vim with the temp file
                temp_filename = os.path.basename(temp_path)
                print(f"RaySSH: ✏️  Opening vim with temp file: {temp_filename}")
                vim_process = subprocess.run(['vim', temp_path])

                # Check if vim exited successfully
                if vim_process.returncode != 0:
                    print(f"vim exited with code {vim_process.returncode}", file=sys.stderr)
                    return True

                # Check if file was modified
                new_mtime = os.path.getmtime(temp_path)
                if new_mtime == original_mtime:
                    print("RaySSH: ℹ️  No changes made")
                    return True

                # Read the modified content
                with open(temp_path, encoding='utf-8') as f:
                    modified_content = f.read()

                # Write back to remote
                print(f"RaySSH: 📤 Syncing changes to remote: {filename}")
                write_result = ray.get(self.shell_actor.write_file.remote(filename, modified_content))

                if write_result['success']:
                    print("RaySSH: 🎉 File synchronized successfully")
                else:
                    print(f"Error writing to remote: {write_result['error']}", file=sys.stderr)
                    return True

            finally:
                # Clean up temp file
                try:
                    os.unlink(temp_path)
                except OSError:
                    pass

            return True

        except KeyboardInterrupt:
            print("\nVim session interrupted")
            return True
        except FileNotFoundError:
            print("Error: vim not found. Please install vim on your local system.", file=sys.stderr)
            return True
        except Exception as e:
            print(f"Error in vim session: {e}", file=sys.stderr)
            return True

    def _is_interactive_command(self, command: str) -> bool:
        """
        Check if a command is likely to be interactive (needs stdin).
        """
        interactive_programs = {
            'python', 'python3', 'node', 'nodejs', 'ruby', 'irb', 'php',
            'mysql', 'psql', 'sqlite3', 'redis-cli', 'mongo', 'bc',
            'ftp', 'telnet', 'ssh', 'less', 'more', 'top', 'htop',
            'vi', 'nano', 'emacs', 'pico'
        }

        # Get the base command (first word)
        cmd_parts = command.strip().split()
        if not cmd_parts:
            return False

        base_cmd = os.path.basename(cmd_parts[0])

        # Check if it's a known interactive program
        if base_cmd in interactive_programs:
            return True

        # Check for some patterns that suggest interactivity
        if any(pattern in command.lower() for pattern in ['-i ', '--interactive']):
            return True

        return False

    def _handle_interactive_command(self, command: str) -> bool:
        """
        Handle an interactive command that needs stdin support.

        Returns:
            True to continue, False to exit
        """
        try:
            print()
            print("=" * 60)
            print(f"🔄 RaySSH: Starting interactive session: {command}")
            print("=" * 60)

            # Start the interactive command
            start_result = ray.get(self.shell_actor.start_interactive_command.remote(command))

            if not start_result['success']:
                print(f"Failed to start interactive command: {start_result['error']}", file=sys.stderr)
                return True

            session_id = start_result['session_id']
            self.current_interactive_session = session_id  # Track for Ctrl-C handling
            print("⚡ Interactive session started (Ctrl-C to interrupt)")
            print("-" * 60)

            try:
                # Use threading for concurrent input/output handling
                input_queue = queue.Queue()

                def input_reader():
                    """Read input from stdin in separate thread with no local echo"""
                    # Set up terminal for raw input (no echo)
                    fd = sys.stdin.fileno()
                    old_settings = termios.tcgetattr(fd)
                    try:
                        # Set raw mode but keep some basic terminal features
                        new_settings = termios.tcgetattr(fd)
                        new_settings[3] &= ~(termios.ECHO | termios.ICANON)  # Disable echo and canonical mode
                        new_settings[6][termios.VMIN] = 1  # Minimum characters to read
                        new_settings[6][termios.VTIME] = 0  # Timeout
                        termios.tcsetattr(fd, termios.TCSADRAIN, new_settings)
                        
                        while self.current_interactive_session == session_id:
                            try:
                                char = sys.stdin.read(1)
                                if not char:
                                    break
                                
                                # Send each character immediately to let remote PTY handle echoing
                                if char == '\x03':  # Ctrl-C
                                    input_queue.put(None)
                                    break
                                elif char == '\x04':  # Ctrl-D (EOF)
                                    input_queue.put(None)
                                    break
                                else:
                                    # Send character immediately (including Enter, Backspace, etc.)
                                    input_queue.put(char)
                                    
                            except (EOFError, KeyboardInterrupt):
                                input_queue.put(None)
                                break
                            except Exception:
                                break
                                
                    finally:
                        # Restore terminal settings
                        termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)

                # Start input reader thread
                input_thread = threading.Thread(target=input_reader, daemon=True)
                input_thread.start()

                # Main interactive loop
                finished = False

                while not finished and not self.shutdown_event.is_set() and self.current_interactive_session:
                    # Read output from remote process
                    output_result = ray.get(self.shell_actor.read_output_chunk.remote(session_id, 0.1))

                    if output_result['success']:
                        # Display any output
                        if output_result['stdout']:
                            filtered_stdout = self._filter_raylet_warnings(output_result['stdout'])
                            if filtered_stdout:
                                print(filtered_stdout, end='', flush=True)
                        if output_result['stderr']:
                            filtered_stderr = self._filter_raylet_warnings(output_result['stderr'])
                            if filtered_stderr:
                                print(filtered_stderr, end='', flush=True, file=sys.stderr)

                        finished = output_result['finished']
                        if finished:
                            returncode = output_result['returncode']
                            if returncode and returncode != 0:
                                print(f"\nRaySSH: Process exited with code {returncode}")
                            break

                    # Check for user input (non-blocking)
                    try:
                        while not input_queue.empty():
                            user_input = input_queue.get_nowait()
                            if user_input is None:  # EOF or interrupt
                                finished = True
                                break

                            # Send input to remote process
                            send_result = ray.get(self.shell_actor.send_stdin_data.remote(session_id, user_input))
                            if not send_result['success']:
                                if "terminated" in send_result['error']:
                                    finished = True
                                    break
                                else:
                                    print(f"Error sending input: {send_result['error']}", file=sys.stderr)
                    except queue.Empty:
                        pass

                    # Small delay to prevent busy waiting
                    time.sleep(0.01)

            except KeyboardInterrupt:
                print()
                print("-" * 60)
                print("🛑 RaySSH: Interactive session interrupted")
                print("-" * 60)

            finally:
                # Clean up the session
                self.current_interactive_session = None
                try:
                    ray.get(self.shell_actor.terminate_interactive_command.remote(session_id))
                except Exception:
                    pass
                print("-" * 60)
                print("✅ RaySSH: Interactive session ended")
                print("=" * 60)

            return True

        except Exception as e:
            print(f"Error in interactive session: {e}", file=sys.stderr)
            return True

    def execute_command(self, command: str) -> bool:
        """
        Execute a command on the remote shell.

        Returns:
            True to continue, False to exit
        """
        if command.strip().lower() in ['exit', 'quit']:
            return False

        # Check for vim command
        if command.strip().startswith('vim '):
            return self._handle_vim_command(command)

        # Check if this is an interactive command
        if self._is_interactive_command(command):
            return self._handle_interactive_command(command)

        try:
            # Execute command asynchronously so we can handle signals
            self.current_command_future = self.shell_actor.execute_command.remote(command)
            result = ray.get(self.current_command_future)
            self.current_command_future = None

            # Print stdout and stderr with raylet warning filtering
            if result['stdout']:
                filtered_stdout = self._filter_raylet_warnings(result['stdout'])
                if filtered_stdout:
                    print(filtered_stdout, end='')
            if result['stderr']:
                filtered_stderr = self._filter_raylet_warnings(result['stderr'])
                if filtered_stderr:
                    print(filtered_stderr, end='', file=sys.stderr)

            return True

        except KeyboardInterrupt:
            # This shouldn't happen due to our signal handling, but just in case
            print("\n^C")
            return True
        except Exception as e:
            print(f"Error executing command: {e}", file=sys.stderr)
            return True

    def run_interactive_shell(self):
        """Run the main interactive shell loop."""
        print("Connected! Type 'exit' or 'quit' to disconnect, Ctrl-C to interrupt commands, Ctrl-D to exit.")
        if READLINE_AVAILABLE:
            print("Tab completion is enabled for file names.")
        print()

        # Set up tab completion
        self._setup_tab_completion()

        try:
            while not self.shutdown_event.is_set():
                try:
                    # Get and display prompt
                    prompt = self.get_prompt()

                    # Read command with proper handling of Ctrl-D
                    try:
                        command = input(prompt)
                    except EOFError:
                        # Ctrl-D pressed
                        print("\nExiting...")
                        break

                    # Execute command
                    if not self.execute_command(command):
                        break

                except KeyboardInterrupt:
                    # This should be handled by signal handler, but just in case
                    continue

        except Exception as e:
            print(f"Shell error: {e}", file=sys.stderr)
        finally:
            # Clean up tab completion
            self._cleanup_tab_completion()

        print("Shell session ended.")

    def cleanup(self):
        """Clean up resources."""
        # Clean up tab completion
        self._cleanup_tab_completion()

        if self.shell_actor:
            try:
                # Clean up any running processes in the shell actor
                ray.get(self.shell_actor.cleanup.remote())

                # Kill the actor
                ray.kill(self.shell_actor)
            except Exception:
                pass

        # Shutdown Ray if we initialized it
        if ray.is_initialized():
            try:
                ray.shutdown()
            except Exception:
                pass


def get_ordered_nodes():
    """
    Get Ray nodes in the same order as displayed in --ls table.
    Returns (nodes, head_node_index) where nodes is list of alive nodes
    and head_node_index is the index of the head node.
    """
    # Ensure Ray is initialized
    ensure_ray_initialized()

    # Get all nodes in the cluster
    all_nodes = get_ray_cluster_nodes()

    if not all_nodes:
        return [], -1

    # Filter to only show alive nodes
    nodes = [node for node in all_nodes if node.get('Alive', False)]

    if not nodes:
        return [], -1

    # Detect head node (same logic as print_nodes_table)
    head_node_id = None
    head_node_index = -1

    for i, node in enumerate(nodes):
        resources = node.get('Resources', {})
        # Head node typically has 'node:' resources or is the first node
        if any(key.startswith('node:') for key in resources.keys()) or head_node_id is None:
            head_node_id = node.get('NodeID')
            head_node_index = i
            break

    return nodes, head_node_index

def get_node_by_index(index: int):
    """
    Get a node by its index in the --ls table.
    -0 = head node, -1 = first non-head node, etc.

    Returns the node dict or raises ValueError if index is invalid.
    """
    nodes, head_node_index = get_ordered_nodes()

    if not nodes:
        raise ValueError("No alive Ray nodes found in the cluster")

    if index == 0:
        # -0 means head node
        if head_node_index >= 0:
            return nodes[head_node_index]
        else:
            raise ValueError("No head node found")

    # For non-head nodes, create a list excluding the head node
    non_head_nodes = []
    for i, node in enumerate(nodes):
        if i != head_node_index:
            non_head_nodes.append(node)

    # -1 = first non-head, -2 = second non-head, etc.
    non_head_index = index - 1

    if non_head_index < 0 or non_head_index >= len(non_head_nodes):
        raise ValueError(f"Node index -{index} is out of range. Available: -0 to -{len(non_head_nodes)}")

    return non_head_nodes[non_head_index]

def print_nodes_table():
    """Print a table of available Ray nodes."""
    try:
        # Ensure Ray is initialized
        ensure_ray_initialized()

        # Get all nodes in the cluster
        all_nodes = get_ray_cluster_nodes()

        if not all_nodes:
            print("🚫 No Ray nodes found in the cluster.")
            return

        # Filter to only show alive nodes
        nodes = [node for node in all_nodes if node.get('Alive', False)]

        if not nodes:
            print("🚫 No alive Ray nodes found in the cluster.")
            return

        print(f"🌐 Ray Cluster Nodes ({len(nodes)} alive)")
        print("=" * 85)

        # Print table header
        print(f"{'📍 Node':<12} {'🌍 IP Address':<16} {'🆔 ID':<8} {'🖥️  CPU':<12} {'🎮 GPU':<12} {'💾 Memory':<12}")
        print("-" * 85)

        # Detect head node (usually the first node or one with special characteristics)
        head_node_id = None
        for node in nodes:
            resources = node.get('Resources', {})
            # Head node typically has 'node:' resources or is the first node
            if any(key.startswith('node:') for key in resources.keys()) or head_node_id is None:
                head_node_id = node.get('NodeID')
                break

        # Print each node in table format
        for i, node in enumerate(nodes, 1):
            node_ip = node.get('NodeManagerAddress', 'N/A')
            full_node_id = node.get('NodeID', 'N/A')
            node_id = f"{full_node_id[:6]}..." if full_node_id != 'N/A' else 'N/A'  # Show first 6 chars with ellipsis

            # Mark head node
            is_head_node = node.get('NodeID') == head_node_id
            node_label = f"{'👑 Head' if is_head_node else str(i)}"

            # Get resources
            resources = node.get('Resources', {})

            # CPU usage formatting
            cpu_total = int(resources.get('CPU', 0)) if resources.get('CPU', 0) else 0
            cpu_display = f"{cpu_total}" if cpu_total > 0 else "0"

            # GPU usage formatting
            gpu_total = int(resources.get('GPU', 0)) if resources.get('GPU', 0) else 0
            gpu_display = f"{gpu_total}" if gpu_total > 0 else "0"

            # Memory formatting
            memory_bytes = resources.get('memory', 0)
            memory_gb = f"{memory_bytes / (1024**3):.1f}GB" if memory_bytes > 0 else "0GB"

            print(f"{node_label:<12} {node_ip:<16} {node_id:<8} {cpu_display:<12} {gpu_display:<12} {memory_gb:<12}")

            # Print additional special resources if they exist
            special_resources = []
            for key, value in sorted(resources.items()):
                if key not in ['CPU', 'GPU', 'memory', 'object_store_memory'] and not key.startswith('node:'):
                    if 'accelerator' in key.lower() or 'tpu' in key.lower():
                        special_resources.append(f"⚡ {key}: {int(value) if isinstance(value, float) and value.is_integer() else value}")
                    else:
                        special_resources.append(f"🔧 {key}: {int(value) if isinstance(value, float) and value.is_integer() else value}")

            if special_resources:
                print(f"{'':12} {'└─':16} {', '.join(special_resources)}")

        print("=" * 85)
        print("💡 Use: rayssh <ip_address> or rayssh <node_id_prefix> or rayssh -<index> to connect")
        print("👑 Head node is -0, first non-head is -1, second non-head is -2, etc.")

    except Exception as e:
        print(f"❌ Error listing nodes: {e}", file=sys.stderr)
        return 1

    return 0


def print_help():
    """Print help information."""
    help_text = """
RaySSH: Command Ray nodes like a shell or submit jobs

Usage:
    rayssh                          # Interactive node selection
    rayssh <ip|node_id|-index>      # Connect to specific node
    rayssh --list                   # List available nodes
    rayssh [-q] <file>              # Submit file as Ray job (experimental)

Options:
    -h, --help                      # Show help
    -l, --list, --ls, --show        # List nodes
    -q                              # Quick mode (no-wait for jobs)

Examples:
    rayssh                          # Interactive menu
    rayssh 192.168.1.100            # Connect by IP
    rayssh -0                       # Connect to head node
    rayssh -1                       # Connect to first worker
    rayssh -l                       # Show all nodes
    rayssh script.py                # Submit Python job (tails log)
    rayssh -q train.sh              # Submit bash job (no-wait, view log at Ray Dashboard)

🖥️ Shell features: vim remote files, tab completion, Ctrl-C interrupts, interactive programs
🚀 Job submission: Python/Bash files, working-dir='.', entrypoint-num-cpus=1
"""
    print(help_text.strip())


def interactive_node_selector():
    """
    Display an interactive node selection screen with arrow key navigation.
    Returns the selected node's IP address or None if cancelled.
    """
    try:
        # Get nodes in the same order as --ls table
        nodes, head_node_index = get_ordered_nodes()

        if not nodes:
            print("🚫 No Ray nodes found in the cluster.")
            return None

        # Create display list with proper indexing
        display_nodes = []
        for i, node in enumerate(nodes):
            node_ip = node.get('NodeManagerAddress', 'N/A')
            node_id = node.get('NodeID', 'N/A')[:6]
            is_head = (i == head_node_index)

            # Get resources for display
            resources = node.get('Resources', {})
            cpu = int(resources.get('CPU', 0)) if resources.get('CPU', 0) else 0
            gpu = int(resources.get('GPU', 0)) if resources.get('GPU', 0) else 0
            memory_gb = resources.get('memory', 0) / (1024**3) if resources.get('memory') else 0

            # Get special resources (accelerators, etc.)
            special_resources = []
            for key, value in sorted(resources.items()):
                if key not in ['CPU', 'GPU', 'memory', 'object_store_memory'] and not key.startswith('node:'):
                    if 'accelerator' in key.lower() or 'tpu' in key.lower():
                        special_resources.append(f"{key}: {int(value) if isinstance(value, float) and value.is_integer() else value}")

            display_nodes.append({
                'node': node,
                'ip': node_ip,
                'id': node_id,
                'is_head': is_head,
                'cpu': cpu,
                'gpu': gpu,
                'memory': f"{memory_gb:.1f}GB",
                'special': ', '.join(special_resources) if special_resources else '',
                'index': 0 if is_head else len([n for j, n in enumerate(nodes[:i]) if j != head_node_index]) + 1
            })

        # Simple numbered selection instead of arrow keys
        os.system('clear')
        print("RaySSH: Node Selection")
        print("=" * 50)
        print()
        
        # Display nodes as a numbered list with 0-based indexing matching command line
        print("    #    Type     IP Address       ID       CPU    GPU    Memory")
        print("-" * 60)
        
        for i, display_node in enumerate(display_nodes):
            # Use 0 for head node, then 1, 2, 3... for non-head nodes
            if display_node['is_head']:
                node_number = 0
                node_type = "HEAD"
            else:
                node_number = display_node['index']  # This already has the correct -1, -2, -3... logic
                node_type = f"-{display_node['index']}"
            
            print(f"    {node_number:<4} {node_type:<8} {display_node['ip']:<16} {display_node['id']:<8} {display_node['cpu']:<6} {display_node['gpu']:<6} {display_node['memory']:<8}")
            
            # Show special resources if they exist
            if display_node['special']:
                print(f"         └─ {display_node['special']}")
        
        print()
        print("=" * 50)
        print("Enter node number to connect (0=head, 1+=non-head, 'q' to cancel):")
        
        try:
            choice = input("> ").strip()
            if choice.lower() in ['q', 'quit', 'exit']:
                return None
            if choice == '':
                return None
            
            node_num = int(choice)
            
            # Find the node with the matching number
            selected_node = None
            for display_node in display_nodes:
                if display_node['is_head'] and node_num == 0:
                    selected_node = display_node
                    break
                elif not display_node['is_head'] and node_num == display_node['index']:
                    selected_node = display_node
                    break
            
            if selected_node:
                print(f"Connecting to {selected_node['ip']}...")
                return selected_node['ip']
            else:
                # Show available numbers for error message
                available_nums = []
                for display_node in display_nodes:
                    if display_node['is_head']:
                        available_nums.append("0")
                    else:
                        available_nums.append(str(display_node['index']))
                print(f"Invalid selection. Available: {', '.join(available_nums)} or 'q' to cancel.")
                return None
                
        except (ValueError, KeyboardInterrupt):
            return None

    except Exception as e:
        print(f"Error in node selector: {e}", file=sys.stderr)
        return None


def submit_file_job(file_path: str, no_wait: bool = False) -> int:
    """
    Submit a file as a Ray job (experimental feature).
    
    Args:
        file_path: Path to the file to execute
        no_wait: If True, don't wait for job completion
        
    Returns:
        Exit code (0 for success, 1 for error)
    """
    try:
        # Ensure Ray is initialized
        ensure_ray_initialized()
        
        # Validate file path restrictions
        if not os.path.exists(file_path):
            print(f"Error: File '{file_path}' not found", file=sys.stderr)
            return 1
            
        # Check if file is within current working directory
        abs_file_path = os.path.abspath(file_path)
        abs_cwd = os.path.abspath('.')
        
        if not abs_file_path.startswith(abs_cwd):
            print("Error: File must be within current working directory (experimental restriction)", file=sys.stderr)
            return 1
            
        # Check if it's a text file (not binary)
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                f.read(1024)  # Read first 1KB to check if it's text
        except UnicodeDecodeError:
            print("Error: Binary files are not supported (experimental restriction)", file=sys.stderr)
            return 1
            
        # Determine the interpreter based on file extension
        file_extension = os.path.splitext(file_path)[1].lower()
        if file_extension == '.py':
            interpreter = 'python'
        elif file_extension in ['.sh', '.bash']:
            interpreter = 'bash'
        else:
            # Try to detect shebang
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    first_line = f.readline().strip()
                    if first_line.startswith('#!'):
                        if 'python' in first_line:
                            interpreter = 'python'
                        elif 'bash' in first_line or 'sh' in first_line:
                            interpreter = 'bash'
                        else:
                            print("Error: Unsupported interpreter in shebang. Only Python and Bash are supported.", file=sys.stderr)
                            return 1
                    else:
                        print("Error: Cannot determine interpreter. Use .py or .sh/.bash extension, or add shebang.", file=sys.stderr)
                        return 1
            except Exception as e:
                print(f"Error reading file: {e}", file=sys.stderr)
                return 1
        
        # Build Ray job submit command
        cmd = [
            'ray', 'job', 'submit',
            '--entrypoint-num-cpus=1',
            '--working-dir=.',
            '--'
        ]
        
        if no_wait:
            cmd.insert(-1, '--no-wait')  # Insert before '--'
            
        cmd.extend([interpreter, file_path])
        
        print(f"🚀 RaySSH: Submitting {interpreter} job: {file_path}")
        print(f"📋 Command: {' '.join(cmd)}")
        print("⚠️  Experimental feature - file execution via Ray job submission")
        print()
        
        # Execute the ray job submit command
        result = subprocess.run(cmd, cwd='.')
        return result.returncode
        
    except Exception as e:
        print(f"Error submitting job: {e}", file=sys.stderr)
        return 1


def main():
    """Main entry point for RaySSH."""
    # Parse command line arguments
    if len(sys.argv) < 2:
        # No arguments - show interactive node selector
        selected_node_ip = interactive_node_selector()
        if selected_node_ip is None:
            print("\nCancelled.")
            return 0
        node_arg = selected_node_ip
    else:
        # Handle help command
        if sys.argv[1] in ['--help', '-h']:
            print_help()
            return 0

        # Handle list nodes command
        if sys.argv[1] in ['--list', '--ls', '--show', '-l']:
            if len(sys.argv) != 2:
                print("Error: --list, --ls, --show, and -l options do not accept additional arguments", file=sys.stderr)
                return 1
            return print_nodes_table()

        # Handle file job submission (experimental feature)
        # Check for patterns: rayssh <file> or rayssh -q <file>
        file_job_submission = False
        no_wait = False
        file_path = None
        
        if len(sys.argv) == 2:
            # Pattern: rayssh <file>
            potential_file = sys.argv[1]
            # Check if it looks like a file (has extension and exists)
            if ('.' in potential_file and 
                os.path.exists(potential_file) and 
                os.path.isfile(potential_file)):
                file_job_submission = True
                file_path = potential_file
        elif len(sys.argv) == 3 and sys.argv[1] == '-q':
            # Pattern: rayssh -q <file>
            potential_file = sys.argv[2]
            if ('.' in potential_file and 
                os.path.exists(potential_file) and 
                os.path.isfile(potential_file)):
                file_job_submission = True
                no_wait = True
                file_path = potential_file
        
        if file_job_submission:
            return submit_file_job(file_path, no_wait)

        # Handle node connection
        if len(sys.argv) != 2:
            print("Error: Invalid arguments", file=sys.stderr)
            print("Usage: rayssh <node|file> or rayssh -q <file>", file=sys.stderr)
            print("Use 'rayssh --help' for more information", file=sys.stderr)
            return 1

        node_arg = sys.argv[1]

    # Handle node index argument (-0, -1, -2, etc.)
    if node_arg.startswith('-') and node_arg[1:].isdigit():
        try:
            index = int(node_arg[1:])  # Extract number after '-'
            node = get_node_by_index(index)
            # Use the node's IP address as the connection target
            node_arg = node.get('NodeManagerAddress')
            print(f"Connecting to node -{index}: {node_arg}")
        except ValueError as e:
            print(f"Error: {e}", file=sys.stderr)
            print("Use 'rayssh --ls' to see available nodes", file=sys.stderr)
            return 1
        except Exception as e:
            print(f"Error getting node by index: {e}", file=sys.stderr)
            return 1

    # Create and run the client
    client = RaySSHClient(node_arg)

    try:
        if not client.initialize():
            return 1

        client.run_interactive_shell()
        return 0

    except KeyboardInterrupt:
        print("\nInterrupted")
        return 1
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    finally:
        client.cleanup()


if __name__ == "__main__":
    sys.exit(main())
