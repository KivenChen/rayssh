#!/usr/bin/env python3

import atexit
import os
import queue
import random
import signal
import subprocess
import sys
import tempfile
import termios
import threading
import time
import tty
import select

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
)


class RaySSHClient:
    """Main client for RaySSH that provides an interactive shell interface."""

    def __init__(self, node_arg: str, working_dir: str = None):
        """Initialize the RaySSH client for the target node."""
        self.node_arg = node_arg
        self.working_dir = working_dir  # Directory to upload for Ray Client connections
        self.shell_actor = None
        self.target_node = None
        self.current_command_future = None
        self.current_interactive_session = None  # Track active interactive session
        self.shutdown_event = threading.Event()
        self.friendly_workspace_path = None  # Track workspace symlink for cleanup
        self.uploaded_workspace_active = False  # Whether a temporary uploaded workspace is active
        self._interrupt_flag = False  # Set by SIGINT to allow non-blocking interruption
        # Cache node info to avoid redundant get_node_info RPCs
        self.node_hostname = None
        self.node_user = None
        self.cached_cwd = None
        # Check if we're in remote mode (using RAY_ADDRESS)
        ray_address_from_env = os.environ.get('RAY_ADDRESS')
        self.is_remote_mode = (ray_address_from_env and node_arg == ray_address_from_env)

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
                try:
                    self.shell_actor.interrupt_current_command.remote()
                except Exception:
                    pass
            finally:
                # Propagate to abort blocking waits (e.g., input or ray.get)
                raise KeyboardInterrupt
        elif self.current_interactive_session:
            # Interrupt the interactive session instead of exiting shell
            try:
                # Fire and forget; do not block here
                self.shell_actor.terminate_interactive_command.remote(self.current_interactive_session)
                self.current_interactive_session = None
            finally:
                raise KeyboardInterrupt
        else:
            # At idle prompt: just raise to abort input() and discard current line
            raise KeyboardInterrupt

    def _handle_sigterm(self, signum, frame):
        """Handle SIGTERM."""
        print("\n🛑 Terminating...")
        self.shutdown_event.set()

    def initialize(self):
        """Initialize Ray and set up the shell actor on the target node."""
        if self.is_remote_mode:
            return self._initialize_remote_mode()
        else:
            return self._initialize_local_cluster()

    def _initialize_remote_mode(self):
        """Initialize remote connection with optional working directory upload."""
        try:
            # Initialize remote connection with working directory
            if self.working_dir:
                abs_working_dir = os.path.abspath(self.working_dir)
                print(f"🌐 Connecting to remote cluster and uploading {os.path.basename(abs_working_dir)}...")
            else:
                print(f"🌐 Initiating Ray client connection...")
                
            ensure_ray_initialized(ray_address=self.node_arg, working_dir=self.working_dir)
            
            # Deploy shell actor for remote mode
            print(f"🌐 Deploying shell actor to remote...")
            self.shell_actor = ShellActor.remote()

            # Determine optional project name for one-shot bootstrap
            project_name = None
            if self.working_dir:
                project_name = os.path.basename(os.path.abspath(self.working_dir))
                if project_name == '.':
                    project_name = os.path.basename(os.path.dirname(os.path.abspath(self.working_dir))) or 'workspace'

            # One RPC to fetch node info and optionally create workspace
            boot = ray.get(self.shell_actor.bootstrap.remote(project_name))
            node_info = boot.get('node_info', {})
            workspace_info = boot.get('workspace_info')

            # Cache node info
            self.node_hostname = node_info.get('hostname')
            self.node_user = node_info.get('user')
            self.cached_cwd = node_info.get('cwd')

            if workspace_info and workspace_info.get('success'):
                self.friendly_workspace_path = workspace_info['friendly_path']
                self.uploaded_workspace_active = True
                print(f"🔗 Remote shell ready at ~/.rayssh/workdirs/{workspace_info['project_name']} on {self.node_hostname or '?'}")
                print("⚠️ Note: This workspace is temporary. Changes stay if you keep the session, but uploads are cleaned by Ray when sessions end.")
            else:
                print(f"🔗 Remote shell ready on {self.node_hostname or '?'}")
                if workspace_info and not workspace_info.get('success'):
                    print(f"⚠️  Could not create friendly workspace link: {workspace_info.get('error', 'Unknown error')}")
            print()

            return True
            
        except Exception as e:
            print(f"❌ Error connecting to remote cluster: {e}", file=sys.stderr)
            return False

    def _initialize_local_cluster(self):
        """Initialize connection to local Ray cluster."""
        # Ensure Ray is initialized
        ensure_ray_initialized()

        # Find the target node
        try:
            self.target_node = find_target_node(self.node_arg)
            node_resources = get_node_resources(self.target_node)
            print(f"🎯 Found target node: {node_resources['node_ip']} (ID: {node_resources['node_id'][:8]}...)")
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
            print(f"✅ Shell actor deployed on {node_info['hostname']} (user: {node_info['user']})")
            print(f"📁 Initial working directory: {node_info['cwd']}")
            print()

            # Cache node info
            self.node_hostname = node_info.get('hostname')
            self.node_user = node_info.get('user')
            self.cached_cwd = node_info.get('cwd')

        except Exception as e:
            print(f"Error deploying shell actor: {e}", file=sys.stderr)
            return False

        return True

    def get_prompt(self) -> str:
        """Generate a shell prompt."""
        try:
            hostname = self.node_hostname or "?"
            user = self.node_user or "user"
            cwd = self.cached_cwd or "~"

            # Shorten home directory path using the real username
            home = f"/home/{user}"
            if cwd.startswith(home):
                if cwd == home:
                    display_cwd = "~"
                else:
                    relative_path = cwd[len(home):].lstrip('/')
                    if '/' in relative_path:
                        display_cwd = os.path.basename(relative_path)
                    else:
                        display_cwd = relative_path
            else:
                display_cwd = os.path.basename(cwd) if cwd != '/' else '/'

            return f"RaySSH@{hostname}:{display_cwd}$ "
        except Exception:
            return "rayssh$ "

    def _setup_tab_completion(self) -> bool:
        """Set up tab completion for file names."""
        if not READLINE_AVAILABLE:
            return False

        # Lightweight in-memory cache for the last directory listing
        last_listing = {
            'dir': None,
            'entries': [],
            'timestamp': 0.0,
            'include_hidden': True,
        }

        # Cache for the last prefix to avoid recomputing matches when cycling with TAB
        last_match_cache = {
            'key': None,  # (dir, prefix, absolute_flag)
            'matches': []
        }

        # Cache for current working directory to avoid frequent remote calls
        last_cwd_cache = {
            'cwd': None,
            'timestamp': 0.0,
        }

        # Time-to-live for caches (seconds)
        CACHE_TTL_SEC = 0.35

        def complete_filenames(text, state):
            """Complete file names in the current directory."""
            try:
                # Get current working directory from local cache (avoid extra RPC)
                now = time.time()
                current_dir = self.cached_cwd or last_cwd_cache.get('cwd') or '/'
                last_cwd_cache['cwd'] = current_dir
                last_cwd_cache['timestamp'] = now

                # Handle relative paths
                if text.startswith('/'):
                    # Absolute path
                    search_dir = os.path.dirname(text) or '/'
                    search_pattern = os.path.basename(text)
                    absolute_flag = True
                elif '/' in text:
                    # Relative path with directories
                    search_dir = os.path.join(current_dir, os.path.dirname(text))
                    search_pattern = os.path.basename(text)
                    absolute_flag = False
                else:
                    # Just a filename
                    search_dir = current_dir
                    search_pattern = text
                    absolute_flag = False

                # Prepare cache key
                cache_key = (search_dir, search_pattern, absolute_flag)

                # If cache key changed, recompute matches
                if last_match_cache['key'] != cache_key:
                    # Directory listing caching (avoid repeated remote calls)
                    include_hidden = True if search_pattern.startswith('.') else False
                    need_listing = (
                        last_listing['dir'] != search_dir or
                        (now - last_listing['timestamp'] > CACHE_TTL_SEC) or
                        (last_listing.get('include_hidden') != include_hidden)
                    )
                    if need_listing:
                        listing = ray.get(self.shell_actor.list_directory.remote(search_dir, include_hidden))
                        if not listing.get('success', False):
                            return None
                        # Store listing in cache
                        last_listing['dir'] = search_dir
                        last_listing['entries'] = listing.get('entries', [])
                        last_listing['timestamp'] = now
                        last_listing['include_hidden'] = include_hidden

                    # Build matches from cached listing
                    files = [e['name'] for e in last_listing['entries']]

                    matches = []
                    base_dir = os.path.dirname(text) if '/' in text else ''
                    for file in files:
                        if file and file.startswith(search_pattern):
                            # If it's a directory, append a trailing slash for UX
                            is_dir = False
                            # Try to find 'is_dir' quickly from entries
                            for entry in last_listing['entries']:
                                if entry['name'] == file:
                                    is_dir = bool(entry.get('is_dir'))
                                    break
                            display = file + ('/' if is_dir else '')
                            if base_dir:
                                full_path = os.path.join(os.path.dirname(text), display)
                                matches.append(full_path)
                            else:
                                matches.append(display)

                    last_match_cache['key'] = cache_key
                    last_match_cache['matches'] = matches
                else:
                    matches = last_match_cache['matches']

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
            return False

        return True

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
        # If working in an uploaded temp workspace, show a quick reminder
        if self.uploaded_workspace_active:
            print("⚠️ Temporary workspace: Files are in an uploaded working directory for this session.")

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
            
            # Use cached hostname for temp file naming
            try:
                hostname = self.node_hostname or "remote"
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
            print(f"🔄 Interactive session started")
            # print("⚡ Interactive session started (Ctrl-C to interrupt)")
            print("-" * 60)

            try:
                # Use threading for concurrent input/output handling
                input_queue = queue.Queue()
                # Ensure stale future is cleared so main loop reflects idle state
                self.current_command_future = None

                def input_reader():
                    """Read input from stdin in separate thread with no local echo"""
                    # Set up terminal for raw input (no echo)
                    fd = sys.stdin.fileno()
                    old_settings = termios.tcgetattr(fd)
                    try:
                        # Set raw mode but keep some basic terminal features
                        new_settings = termios.tcgetattr(fd)
                        new_settings[3] &= ~(termios.ECHO | termios.ICANON | termios.ISIG)  # Disable echo, canonical mode, and local signals
                        new_settings[6][termios.VMIN] = 1  # Minimum characters to read
                        new_settings[6][termios.VTIME] = 0  # Timeout
                        termios.tcsetattr(fd, termios.TCSADRAIN, new_settings)
                        
                        # Batch keystrokes into small chunks
                        buf = []
                        last_flush = time.time()
                        FLUSH_AFTER_SEC = 0.015  # 15ms
                        MAX_CHUNK = 256
                        while self.current_interactive_session == session_id:
                            try:
                                # Non-blocking poll for input to allow timely exit
                                rlist, _, _ = select.select([sys.stdin], [], [], 0.05)
                                now_t = time.time()
                                if rlist:
                                    char = sys.stdin.read(1)
                                    if not char:
                                        break
                                    # Ctrl-C: signal remote, do not end local session
                                    if char == '\x03':
                                        try:
                                            ray.get(self.shell_actor.signal_interactive_process.remote(session_id, 2))
                                        except Exception:
                                            pass
                                        # Flush any buffered text before continuing
                                        if buf:
                                            input_queue.put(''.join(buf))
                                            buf.clear()
                                            last_flush = now_t
                                        continue
                                    # Ctrl-D: EOF -> end session
                                    if char == '\x04':
                                        if buf:
                                            input_queue.put(''.join(buf))
                                            buf.clear()
                                        input_queue.put(None)
                                        break
                                    buf.append(char)
                                    # Flush on newline or chunk size
                                    if char in ('\n', '\r') or len(buf) >= MAX_CHUNK:
                                        input_queue.put(''.join(buf))
                                        buf.clear()
                                        last_flush = now_t
                                # Time-based flush
                                if buf and (now_t - last_flush) >= FLUSH_AFTER_SEC:
                                    input_queue.put(''.join(buf))
                                    buf.clear()
                                    last_flush = now_t
                            except (EOFError, KeyboardInterrupt):
                                if buf:
                                    input_queue.put(''.join(buf))
                                    buf.clear()
                                input_queue.put(None)
                                break
                            except Exception:
                                # On unexpected error, flush buffer and exit loop
                                if buf:
                                    input_queue.put(''.join(buf))
                                    buf.clear()
                                break
                          
                    finally:
                        # Restore terminal settings
                        termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
 
                # Start input reader thread
                input_thread = threading.Thread(target=input_reader, daemon=True)
                input_thread.start()
 
                # Main interactive loop
                finished = False
                poll_timeout = 0.05  # adaptive poll timeout
                MIN_TIMEOUT = 0.02
                MAX_TIMEOUT = 0.25
 
                while not finished and not self.shutdown_event.is_set() and self.current_interactive_session:
                    # Read output from remote process with adaptive timeout
                    output_result = ray.get(self.shell_actor.read_output_chunk.remote(session_id, poll_timeout))
 
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
                        # If we got output, tighten polling
                        if output_result['stdout'] or output_result['stderr']:
                            poll_timeout = max(MIN_TIMEOUT, poll_timeout * 0.6)
                        else:
                            poll_timeout = min(MAX_TIMEOUT, poll_timeout * 1.4)
 
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
                    time.sleep(0.005)

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
                # Ensure input reader restored terminal and drain any leftover keystrokes
                try:
                    input_thread.join(timeout=0.2)
                except Exception:
                    pass
                try:
                    fd = sys.stdin.fileno()
                    termios.tcflush(fd, termios.TCIFLUSH)
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

            # Update cached cwd from result if available
            try:
                if isinstance(result, dict) and 'cwd' in result:
                    self.cached_cwd = result['cwd']
            except Exception:
                pass

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
        print("✅ Connected! Type 'exit' or 'quit' to disconnect, Ctrl-C to interrupt, Ctrl-D to exit.")

        # Set up tab completion
        if self._setup_tab_completion():
            print("🔍 Tab completion is enabled for file names.")
        print()

        try:
            while not self.shutdown_event.is_set():
                try:
                    # Get and display prompt
                    prompt = self.get_prompt()

                    # Read command with proper handling of Ctrl-D / Ctrl-C
                    try:
                        command = input(prompt)
                    except EOFError:
                        print("\n👋 Exiting...")
                        break
                    except KeyboardInterrupt:
                        # Discard current line; show a fresh prompt on next loop
                        print()
                        continue

                    # Execute command
                    try:
                        if not self.execute_command(command):
                            break
                    except KeyboardInterrupt:
                        # Print caret-C then continue to repaint prompt
                        print("^C")
                        continue

                except KeyboardInterrupt:
                    # Just in case; prompt will repaint
                    continue
                except Exception as e:
                    # Keep shell alive on unexpected error
                    print(f"Shell loop error: {e}", file=sys.stderr)
                    continue

        except Exception as e:
            print(f"Shell error: {e}", file=sys.stderr)
        finally:
            # Clean up tab completion
            self._cleanup_tab_completion()

    def cleanup(self):
        """Clean up resources."""
        # Clean up tab completion
        self._cleanup_tab_completion()

        # One-shot actor cleanup (workspace + processes)
        if self.shell_actor:
            try:
                ray.get(self.shell_actor.cleanup_all.remote(self.friendly_workspace_path))
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
    # Use shared State API helper; assume Ray is already initialized by callers
    from utils import fetch_cluster_nodes_via_state
    nodes, head_node_id = fetch_cluster_nodes_via_state()
    if not nodes:
        return [], -1

    # Compute head index in this list (fallback to 0 if not found)
    head_node_index = 0
    if head_node_id:
        for i, node in enumerate(nodes):
            if node.get('NodeID') == head_node_id:
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


def get_random_worker_node():
    """
    Get a random worker (non-head) node.
    Returns the node dict or None if no worker nodes are available.
    Raises ValueError if only head node exists.
    """
    nodes, head_node_index = get_ordered_nodes()

    if not nodes:
        raise ValueError("No alive Ray nodes found in the cluster")
    
    # Create list of worker nodes (non-head nodes)
    worker_nodes = []
    for i, node in enumerate(nodes):
        if i != head_node_index:
            worker_nodes.append(node)
    
    if not worker_nodes:
        raise ValueError("Only head node available. Use 'rayssh -0' to connect to head node, or 'rayssh -l' to see all nodes.")
    
    # Randomly select a worker node
    selected_node = random.choice(worker_nodes)
    return selected_node


def print_nodes_table():
    """Print a table of available Ray nodes."""
    try:
        # Fetch and normalize using shared helper
        from utils import fetch_cluster_nodes_via_state
        nodes, head_node_id = fetch_cluster_nodes_via_state()
        if not nodes:
            print("🚫 No Ray nodes found in the cluster.")
            return 0

        print(f"🌐 Ray Cluster Nodes ({len(nodes)} alive)")
        print("=" * 85)

        # Header
        print(f"{'📍 Node':<12} {'🌍 IP Address':<16} {'🆔 ID':<8} {'🖥️  CPU':<12} {'🎮 GPU':<12} {'💾 Memory':<12}")
        print("-" * 85)

        # Print rows
        for i, node in enumerate(nodes, 1):
            node_ip = node.get('NodeManagerAddress', 'N/A') or 'N/A'
            full_node_id = node.get('NodeID', 'N/A') or 'N/A'
            node_id_short = f"{full_node_id[:6]}..." if full_node_id != 'N/A' else 'N/A'

            is_head_node = (full_node_id == head_node_id)
            node_label = f"{'👑 Head' if is_head_node else str(i)}"

            resources = node.get('Resources', {}) or {}
            cpu_total = int(resources.get('CPU', 0)) if resources.get('CPU', 0) else 0
            gpu_total = int(resources.get('GPU', 0)) if resources.get('GPU', 0) else 0

            # memory reported may be bytes; format to GB
            memory_val = resources.get('memory', 0) or 0
            try:
                memory_gb = f"{(float(memory_val) / (1024**3)):.1f}GB" if float(memory_val) > 0 else "0GB"
            except Exception:
                memory_gb = "0GB"

            print(f"{node_label:<12} {node_ip:<16} {node_id_short:<8} {cpu_total:<12} {gpu_total:<12} {memory_gb:<12}")

            # Special resources
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
        print("🎲 Use 'rayssh' (no args) for random worker, 'rayssh -l' for interactive, 'rayssh --ls' for this table")

    except Exception as e:
        print(f"❌ Error listing nodes: {e}", file=sys.stderr)
        return 1

    return 0


def print_help():
    """Print help information."""
    help_text = """
RaySSH: Command Ray nodes like a shell or submit jobs

Usage:
    rayssh                          # Randomly connect to a worker node (or remote mode if RAY_ADDRESS set)
    rayssh <ip|node_id|-index>      # Connect to specific node
    rayssh <dir>                    # Remote mode with directory upload (if RAY_ADDRESS set)
    rayssh -l                       # Interactive node selection
    rayssh --ls                     # Print nodes table
    rayssh [-q] <file>              # Submit file as Ray job (experimental)

Options:
    -h, --help                      # Show help
    -l, --list, --show              # Interactive node selection
    --ls                            # Print nodes table
    -q                              # Quick mode (no-wait for jobs)

Examples:
    rayssh                          # Random worker node connection
    rayssh 192.168.1.100            # Connect by IP
    rayssh -0                       # Connect to head node
    rayssh -1                       # Connect to first worker
    rayssh -l                       # Interactive node selection
    rayssh --ls                     # Show nodes table
    rayssh script.py                # Submit Python job (tails log)
    rayssh -q train.sh              # Submit bash job (no-wait, view log at Ray Dashboard)

Environment Variables:
    RAY_ADDRESS=ray://localhost:10001   # Enable remote mode
    export RAY_ADDRESS=ray://cluster:10001
    rayssh                          # → Connect remotely (no upload)
    rayssh ~/project                # → Connect remotely + upload ~/project
    rayssh .                        # → Connect remotely + upload current directory

🖥️ Shell features: vim remote files, tab completion, Ctrl-C interrupts, interactive programs
🚀 Job submission: Python/Bash files, working-dir='.', entrypoint-num-cpus=1
🌐 Remote mode: Upload local directories, work on remote clusters like local development
"""
    print(help_text.strip())


def interactive_node_selector():
    """
    Display an interactive node selection screen with arrow key navigation.
    Returns the selected node's IP address or None if cancelled.
    """
    try:
        # Get nodes in the same order as --ls table (via State API helper)
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
        print("🎯 RaySSH: Node Selection")
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
        print("🔢 Enter node number to connect (0=head, 1+=non-head, 'q' to cancel):")
        
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
                print(f"🚀 Connecting to {selected_node['ip']}...")
                return selected_node['ip']
            else:
                # Show available numbers for error message
                available_nums = []
                for display_node in display_nodes:
                    if display_node['is_head']:
                        available_nums.append("0")
                    else:
                        available_nums.append(str(display_node['index']))
                print(f"❌ Invalid selection. Available: {', '.join(available_nums)} or 'q' to cancel.")
                return None
                
        except (ValueError, KeyboardInterrupt):
            return None

    except Exception as e:
        print(f"❌ Error in node selector: {e}", file=sys.stderr)
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
        
        # Prepare working dir and runtime env options
        working_dir_opt = '--working-dir=.'
        runtime_env_candidates = ['runtime_env.yaml', 'runtime_env.yml']
        runtime_env_file = next((f for f in runtime_env_candidates if os.path.isfile(f)), None)
        runtime_env_present = runtime_env_file is not None
        
        # Build Ray job submit command
        cmd = [
            'ray', 'job', 'submit',
            '--entrypoint-num-cpus=1',
            working_dir_opt,
        ]
        
        if runtime_env_present:
            cmd.append(f'--runtime-env={runtime_env_file}')
        
        cmd.append('--')
        
        if no_wait:
            # Insert no-wait just before entrypoint
            cmd.insert(cmd.index('--'), '--no-wait')
            
        cmd.extend([interpreter, file_path])
        
        # Print concise context
        print(f"🚀 RaySSH: Submitting {interpreter} job: {file_path}")
        print(f"📦 Working dir: .")
        if runtime_env_present:
            print(f"🧩 Runtime env: ./{runtime_env_file}")
        else:
            print(f"🧩 Runtime env: remote (create runtime_env.yaml or runtime_env.yml to customize)")
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
    # Check if RAY_ADDRESS is set
    ray_address_env = os.environ.get('RAY_ADDRESS')
    
    # Parse command line arguments
    if len(sys.argv) < 2:
        # No arguments
        if ray_address_env:            
            client = RaySSHClient(ray_address_env, working_dir=None)
            
            try:
                if not client.initialize():
                    return 1
                client.run_interactive_shell()
                return 0
            except KeyboardInterrupt:
                print("\n⚡ Interrupted")
                return 1
            except Exception as e:
                print(f"Error: {e}", file=sys.stderr)
                return 1
            finally:
                client.cleanup()
        else:
            # No RAY_ADDRESS or not Ray Client - randomly connect to a worker node
            try:
                selected_node = get_random_worker_node()
                node_arg = selected_node.get('NodeManagerAddress')
                print(f"🎲 Randomly connecting to worker node: {node_arg}")
            except ValueError as e:
                print(f"Error: {e}")
                return 1
            except Exception as e:
                print(f"Error selecting random worker node: {e}", file=sys.stderr)
                return 1
    else:
        # Handle help command
        if sys.argv[1] in ['--help', '-h']:
            print_help()
            return 0

        # Handle list nodes table command - prints the table
        if sys.argv[1] in ['--ls']:
            if len(sys.argv) != 2:
                print("Error: --ls option does not accept additional arguments", file=sys.stderr)
                return 1
            return print_nodes_table()

        # Handle interactive node selector command
        if sys.argv[1] in ['--list', '--show', '-l']:
            if len(sys.argv) != 2:
                print("Error: --list, --show, and -l options do not accept additional arguments", file=sys.stderr)
                return 1
            selected_node_ip = interactive_node_selector()
            if selected_node_ip is None:
                print("\n❌ Cancelled.")
                return 0
            node_arg = selected_node_ip
        else:
            # Handle single argument: could be directory (for remote mode) or file (for job submission)
            if len(sys.argv) == 2:
                argument = sys.argv[1]
                
                # Check if it's a file for job submission
                if (os.path.exists(argument) and os.path.isfile(argument) and '.' in argument):
                    # It's a file - submit as Ray job
                    return submit_file_job(argument, no_wait=False)
                
                # Check if it's a directory and RAY_ADDRESS is set
                elif ray_address_env and os.path.exists(argument) and os.path.isdir(argument):
                    # It's a directory and we're in remote mode - upload and connect
                    print(f"🌐 Initiating Ray client connection...")
                    print(f"📦 Uploading directory: {os.path.abspath(argument)}")
                    
                    client = RaySSHClient(ray_address_env, working_dir=argument)
                    
                    try:
                        if not client.initialize():
                            return 1
                        client.run_interactive_shell()
                        return 0
                    except KeyboardInterrupt:
                        print("\n⚡ Interrupted")
                        return 1
                    except Exception as e:
                        print(f"Error: {e}", file=sys.stderr)
                        return 1
                    finally:
                        client.cleanup()
                
                # Check if it's a directory but no RAY_ADDRESS
                elif os.path.exists(argument) and os.path.isdir(argument):
                    print(f"Error: Directory specified but RAY_ADDRESS not set.", file=sys.stderr)
                    print(f"Set RAY_ADDRESS to enable remote mode with directory upload.", file=sys.stderr)
                    return 1
                
                # Not a file or directory - might be node argument, let it fall through
                
            # Handle -q file pattern
            elif len(sys.argv) == 3 and sys.argv[1] == '-q':
                potential_file = sys.argv[2]
                if (os.path.exists(potential_file) and os.path.isfile(potential_file) and '.' in potential_file):
                    return submit_file_job(potential_file, no_wait=True)
                else:
                    print(f"Error: File '{potential_file}' not found or not a valid file", file=sys.stderr)
                    return 1



            # Handle node connection
            if len(sys.argv) != 2:
                print("Error: Invalid arguments", file=sys.stderr)
                print("Usage: rayssh <node|file|directory> or rayssh -q <file>", file=sys.stderr)
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
            print(f"🔗 Connecting to node -{index}: {node_arg}")
        except ValueError as e:
            print(f"Error: {e}", file=sys.stderr)
            print("Use 'rayssh --ls' to see available nodes", file=sys.stderr)
            return 1
        except Exception as e:
            print(f"Error getting node by index: {e}", file=sys.stderr)
            return 1

    # Create and run the client
    # For local cluster connections, we don't use working_dir
    client = RaySSHClient(node_arg, working_dir=None)

    try:
        if not client.initialize():
            return 1

        client.run_interactive_shell()
        return 0

    except KeyboardInterrupt:
        print("\n⚡ Interrupted")
        return 1
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    finally:
        client.cleanup()


if __name__ == "__main__":
    sys.exit(main())
