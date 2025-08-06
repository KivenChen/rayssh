#!/usr/bin/env python3

import atexit
import os
import queue
import signal
import subprocess
import sys
import tempfile
import threading
import time

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

            # Create temporary file
            with tempfile.NamedTemporaryFile(mode='w', suffix=os.path.splitext(filename)[1], delete=False, encoding='utf-8') as temp_file:
                temp_file.write(content)
                temp_path = temp_file.name

            try:
                # Get original modification time
                original_mtime = os.path.getmtime(temp_path)

                # Launch vim with the temp file
                print("RaySSH: ✏️  Opening vim...")
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
            print(f"RaySSH: 🔄 Starting interactive session: {command}")

            # Start the interactive command
            start_result = ray.get(self.shell_actor.start_interactive_command.remote(command))

            if not start_result['success']:
                print(f"Failed to start interactive command: {start_result['error']}", file=sys.stderr)
                return True

            session_id = start_result['session_id']
            self.current_interactive_session = session_id  # Track for Ctrl-C handling
            print("RaySSH: ⚡ Interactive session started (Ctrl-C to interrupt)")

            try:
                # Use threading for concurrent input/output handling
                input_queue = queue.Queue()

                def input_reader():
                    """Read input from stdin in separate thread"""
                    while self.current_interactive_session == session_id:
                        try:
                            line = input()  # Blocking read for user input
                            input_queue.put(line + '\n')
                        except (EOFError, KeyboardInterrupt):
                            input_queue.put(None)  # Signal end
                            break
                        except Exception:
                            break

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
                print("\nRaySSH: 🛑 Interactive session interrupted")

            finally:
                # Clean up the session
                self.current_interactive_session = None
                try:
                    ray.get(self.shell_actor.terminate_interactive_command.remote(session_id))
                except Exception:
                    pass
                print("RaySSH: ✅ Interactive session ended")

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
        print("💡 Use: rayssh <ip_address> or rayssh <node_id_prefix> to connect")
        print("👑 Head node is marked with crown")

    except Exception as e:
        print(f"❌ Error listing nodes: {e}", file=sys.stderr)
        return 1

    return 0


def print_help():
    """Print help information."""
    help_text = """
🚀 RaySSH: Command a Ray node like a shell

📋 Usage:
    rayssh <node_ip_address | node_id> [options]
    rayssh --list | --ls | --show

📝 Arguments:
    🌍 node_ip_address    IP address of the target Ray node (format: xxx.yyy.zzz.aaa)
    🆔 node_id           Ray node ID or prefix (hexadecimal string, minimum 6 characters)

⚙️  Options:
    ❓ --help, -h        Show this help message and exit
    📊 --list, --ls, --show
                      List all available Ray nodes in a table format

💡 Examples:
     rayssh 192.168.1.100           # Connect to node by IP
     rayssh a1b2c3d4e5f6            # Connect to node by ID prefix (min 6 chars)
     rayssh a1b2c3d4e5f67890abcdef   # Connect to node by full ID
     rayssh --list                  # List all available nodes
     rayssh --ls                    # List all available nodes (alias)
     rayssh --show                  # List all available nodes (alias)

🖥️  Once connected, you can use the remote shell just like a regular shell:
- 📁 Most standard shell commands work (ls, cat, grep, etc.)
- 🏠 Built-in commands: cd, pwd, export, pushd, popd, dirs
- ✏️  vim <file> - Edit remote files locally with vim (transfers file, opens vim, syncs changes back)
- 🔄 Interactive programs (python, node, mysql, etc.) - Full stdin support for interactive sessions
- ⭐ Tab completion for file names (press Tab to autocomplete)
- ⚡ Ctrl-C interrupts the current command
- 🚪 Ctrl-D or 'exit' or 'quit' to disconnect
- 💾 Working directory and environment variables are maintained across commands

⚠️  Note: This is not a full shell implementation. Complex features like pipes,
redirections, job control, and interactive programs may not work as expected.
"""
    print(help_text.strip())


def main():
    """Main entry point for RaySSH."""
    # Parse command line arguments
    if len(sys.argv) < 2:
        print_help()
        return 0

    # Handle help command
    if sys.argv[1] in ['--help', '-h']:
        print_help()
        return 0

    # Handle list nodes command
    if sys.argv[1] in ['--list', '--ls', '--show']:
        if len(sys.argv) != 2:
            print("Error: --list, --ls, and --show options do not accept additional arguments", file=sys.stderr)
            return 1
        return print_nodes_table()

    # Handle node connection
    if len(sys.argv) != 2:
        print("Error: Invalid number of arguments", file=sys.stderr)
        print("Use 'rayssh --help' for usage information", file=sys.stderr)
        return 1

    node_arg = sys.argv[1]

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
