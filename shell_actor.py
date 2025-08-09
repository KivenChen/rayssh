import os
import pty
import subprocess
import threading
import uuid
from typing import Dict, Optional

import ray


@ray.remote
class ShellActor:
    """
    Ray actor that executes shell commands on the target node.
    Maintains shell state including working directory and environment variables.
    """

    def __init__(self):
        """Initialize the shell actor with default state."""
        self.cwd = os.getcwd()
        self.env = os.environ.copy()
        self.shell_pid = None
        self.running_process = None
        self.process_lock = threading.Lock()
        self.dir_stack = []  # Directory stack for pushd/popd
        self.interactive_processes = {}  # session_id -> process info

    def get_node_info(self) -> Dict:
        """Get information about the current node."""
        return {
            'hostname': os.uname().nodename,
            'cwd': self.cwd,
            'pid': os.getpid(),
            'user': os.environ.get('USER', 'unknown')
        }

    def setup_friendly_workspace(self, project_name: str = None) -> Dict:
        """
        Create a friendly soft link to the uploaded directory and change to it.
        
        Args:
            project_name: Name for the workspace link (defaults to 'workspace')
            
        Returns:
            Dict with 'friendly_path', 'original_path', 'success' keys
        """
        try:
            # Default project name
            if not project_name:
                project_name = 'workspace'
            
            # Get home directory and create rayssh workdirs structure
            home_dir = os.path.expanduser('~')
            rayssh_dir = os.path.join(home_dir, '.rayssh')
            workdirs_path = os.path.join(rayssh_dir, 'workdirs')
            
            # Create directories if they don't exist
            os.makedirs(workdirs_path, exist_ok=True)
            
            # Create friendly link path in workdirs
            friendly_path = os.path.join(workdirs_path, project_name)
            original_path = self.cwd
            
            # Remove existing link if it exists
            if os.path.islink(friendly_path):
                os.unlink(friendly_path)
            elif os.path.exists(friendly_path):
                # If it's a real directory, use a different name
                import time
                timestamp = int(time.time())
                project_name = f"{project_name}_{timestamp}"
                friendly_path = os.path.join(workdirs_path, project_name)
            
            # Create soft link
            os.symlink(original_path, friendly_path)
            
            # Change to the friendly path
            self.cwd = friendly_path
            
            return {
                'friendly_path': friendly_path,
                'original_path': original_path,
                'project_name': project_name,
                'success': True
            }
            
        except Exception as e:
            return {
                'friendly_path': None,
                'original_path': self.cwd,
                'project_name': project_name,
                'success': False,
                'error': str(e)
            }

    def cleanup_workspace(self, friendly_path: str) -> bool:
        """
        Clean up the symlink when session ends.
        
        Args:
            friendly_path: Path to the symlink to remove
            
        Returns:
            bool: True if cleanup successful, False otherwise
        """
        try:
            if os.path.islink(friendly_path):
                os.unlink(friendly_path)
                return True
            return False
        except Exception:
            return False

    def execute_command(self, command: str, timeout: Optional[float] = None) -> Dict:
        """
        Execute a shell command and return the result.

        Args:
            command: The shell command to execute
            timeout: Optional timeout in seconds

        Returns:
            Dict with 'stdout', 'stderr', 'returncode', 'cwd' keys
        """
        if not command.strip():
            return {
                'stdout': '',
                'stderr': '',
                'returncode': 0,
                'cwd': self.cwd
            }

        # Handle built-in commands
        if command.strip().startswith('cd '):
            return self._handle_cd_command(command)
        elif command.strip() in ['pwd']:
            return self._handle_pwd_command()
        elif command.strip().startswith('export '):
            return self._handle_export_command(command)
        elif command.strip().startswith('pushd ') or command.strip() == 'pushd':
            return self._handle_pushd_command(command)
        elif command.strip() == 'popd':
            return self._handle_popd_command()
        elif command.strip() == 'dirs':
            return self._handle_dirs_command()

        # Execute external command
        return self._execute_external_command(command, timeout)

    def _handle_cd_command(self, command: str) -> Dict:
        """Handle the 'cd' built-in command."""
        parts = command.strip().split(None, 1)
        if len(parts) == 1:
            # cd with no arguments goes to home directory
            target_dir = os.path.expanduser('~')
        else:
            target_dir = os.path.expanduser(parts[1])

        # Convert relative path to absolute
        if not os.path.isabs(target_dir):
            target_dir = os.path.join(self.cwd, target_dir)

        target_dir = os.path.normpath(target_dir)

        if os.path.isdir(target_dir):
            self.cwd = target_dir
            return {
                'stdout': '',
                'stderr': '',
                'returncode': 0,
                'cwd': self.cwd
            }
        else:
            return {
                'stdout': '',
                'stderr': f"cd: {target_dir}: No such file or directory\n",
                'returncode': 1,
                'cwd': self.cwd
            }

    def _handle_pwd_command(self) -> Dict:
        """Handle the 'pwd' built-in command."""
        return {
            'stdout': self.cwd + '\n',
            'stderr': '',
            'returncode': 0,
            'cwd': self.cwd
        }

    def _handle_export_command(self, command: str) -> Dict:
        """Handle the 'export' built-in command."""
        parts = command.strip().split(None, 1)
        if len(parts) < 2:
            return {
                'stdout': '',
                'stderr': 'export: usage: export VAR=value\n',
                'returncode': 1,
                'cwd': self.cwd
            }

        assignment = parts[1]
        if '=' in assignment:
            var, value = assignment.split('=', 1)
            self.env[var] = value
            return {
                'stdout': '',
                'stderr': '',
                'returncode': 0,
                'cwd': self.cwd
            }
        else:
            return {
                'stdout': '',
                'stderr': 'export: usage: export VAR=value\n',
                'returncode': 1,
                'cwd': self.cwd
            }

    def _handle_pushd_command(self, command: str) -> Dict:
        """Handle the 'pushd' built-in command."""
        parts = command.strip().split(None, 1)

        if len(parts) == 1:
            # pushd with no arguments - swap top two directories on stack
            if len(self.dir_stack) == 0:
                return {
                    'stdout': '',
                    'stderr': 'pushd: no other directory\n',
                    'returncode': 1,
                    'cwd': self.cwd
                }

            # Swap current directory with top of stack
            top_dir = self.dir_stack[-1]
            self.dir_stack[-1] = self.cwd

            # Try to change to the directory
            if os.path.isdir(top_dir):
                self.cwd = top_dir
                # Show the directory stack
                stack_str = ' '.join([self.cwd] + list(reversed(self.dir_stack)))
                return {
                    'stdout': stack_str + '\n',
                    'stderr': '',
                    'returncode': 0,
                    'cwd': self.cwd
                }
            else:
                # Revert the stack change if directory doesn't exist
                self.dir_stack[-1] = top_dir
                return {
                    'stdout': '',
                    'stderr': f"pushd: {top_dir}: No such file or directory\n",
                    'returncode': 1,
                    'cwd': self.cwd
                }
        else:
            # pushd with directory argument
            target_dir = os.path.expanduser(parts[1])

            # Convert relative path to absolute
            if not os.path.isabs(target_dir):
                target_dir = os.path.join(self.cwd, target_dir)

            target_dir = os.path.normpath(target_dir)

            if os.path.isdir(target_dir):
                # Push current directory onto stack
                self.dir_stack.append(self.cwd)
                self.cwd = target_dir

                # Show the directory stack
                stack_str = ' '.join([self.cwd] + list(reversed(self.dir_stack)))
                return {
                    'stdout': stack_str + '\n',
                    'stderr': '',
                    'returncode': 0,
                    'cwd': self.cwd
                }
            else:
                return {
                    'stdout': '',
                    'stderr': f"pushd: {target_dir}: No such file or directory\n",
                    'returncode': 1,
                    'cwd': self.cwd
                }

    def _handle_popd_command(self) -> Dict:
        """Handle the 'popd' built-in command."""
        if len(self.dir_stack) == 0:
            return {
                'stdout': '',
                'stderr': 'popd: directory stack empty\n',
                'returncode': 1,
                'cwd': self.cwd
            }

        # Pop directory from stack
        popped_dir = self.dir_stack.pop()

        # Try to change to the popped directory
        if os.path.isdir(popped_dir):
            self.cwd = popped_dir

            # Show the directory stack if not empty
            if self.dir_stack:
                stack_str = ' '.join([self.cwd] + list(reversed(self.dir_stack)))
                return {
                    'stdout': stack_str + '\n',
                    'stderr': '',
                    'returncode': 0,
                    'cwd': self.cwd
                }
            else:
                return {
                    'stdout': self.cwd + '\n',
                    'stderr': '',
                    'returncode': 0,
                    'cwd': self.cwd
                }
        else:
            # If popped directory doesn't exist, put it back and show error
            self.dir_stack.append(popped_dir)
            return {
                'stdout': '',
                'stderr': f"popd: {popped_dir}: No such file or directory\n",
                'returncode': 1,
                'cwd': self.cwd
            }

    def _handle_dirs_command(self) -> Dict:
        """Handle the 'dirs' built-in command."""
        # Show the directory stack with current directory first
        if self.dir_stack:
            stack_str = ' '.join([self.cwd] + list(reversed(self.dir_stack)))
        else:
            stack_str = self.cwd

        return {
            'stdout': stack_str + '\n',
            'stderr': '',
            'returncode': 0,
            'cwd': self.cwd
        }

    def _execute_external_command(self, command: str, timeout: Optional[float] = None) -> Dict:
        """Execute an external shell command."""
        try:
            # Start the process (acquire lock only for process creation)
            with self.process_lock:
                self.running_process = subprocess.Popen(
                    command,
                    shell=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    cwd=self.cwd,
                    env=self.env,
                    preexec_fn=os.setsid  # Create new process group for signal handling
                )
                # Store process reference for interrupt handling
                current_process = self.running_process

            try:
                # Wait for completion with timeout (without holding lock)
                stdout, stderr = current_process.communicate(timeout=timeout)
                returncode = current_process.returncode

            except subprocess.TimeoutExpired:
                # Kill the process group on timeout
                os.killpg(os.getpgid(current_process.pid), 15)  # SIGTERM = 15
                stdout, stderr = current_process.communicate()
                returncode = -15  # SIGTERM
                stderr += f"\nCommand timed out after {timeout} seconds\n"

            finally:
                # Clear the running process reference
                with self.process_lock:
                    if self.running_process == current_process:
                        self.running_process = None

            return {
                'stdout': stdout,
                'stderr': stderr,
                'returncode': returncode,
                'cwd': self.cwd
            }

        except Exception as e:
            return {
                'stdout': '',
                'stderr': f"Error executing command: {str(e)}\n",
                'returncode': 1,
                'cwd': self.cwd
            }

    def interrupt_current_command(self) -> bool:
        """
        Interrupt the currently running command (equivalent to Ctrl-C).

        Returns:
            True if a command was interrupted, False otherwise
        """
        with self.process_lock:
            if self.running_process and self.running_process.poll() is None:
                try:
                    # Send SIGINT to the process group
                    pid = self.running_process.pid
                    pgid = os.getpgid(pid)
                    os.killpg(pgid, 2)  # SIGINT = 2
                    return True
                except (ProcessLookupError, OSError) as e:
                    # Process already terminated or other error
                    print(f"Warning: Could not interrupt process: {e}")
                except Exception as e:
                    # Catch any other exception
                    print(f"Error interrupting command: {e}")
        return False

    def suspend_current_command(self) -> bool:
        """
        Suspend the currently running command (equivalent to Ctrl-Z).

        Returns:
            True if a command was suspended, False otherwise
        """
        with self.process_lock:
            if self.running_process and self.running_process.poll() is None:
                try:
                    # Send SIGTSTP to the process group
                    os.killpg(os.getpgid(self.running_process.pid), 20)  # SIGTSTP = 20
                    return True
                except (ProcessLookupError, OSError):
                    # Process already terminated
                    pass
        return False

    def get_current_directory(self) -> str:
        """Get the current working directory."""
        return self.cwd

    def set_current_directory(self, path: str) -> bool:
        """
        Set the current working directory.

        Returns:
            True if successful, False otherwise
        """
        try:
            abs_path = os.path.abspath(os.path.expanduser(path))
            if os.path.isdir(abs_path):
                self.cwd = abs_path
                return True
        except Exception:
            pass
        return False

    def get_environment_variable(self, var_name: str) -> Optional[str]:
        """Get an environment variable value."""
        return self.env.get(var_name)

    def set_environment_variable(self, var_name: str, value: str) -> None:
        """Set an environment variable."""
        self.env[var_name] = value

    def list_environment_variables(self) -> Dict[str, str]:
        """Get all environment variables."""
        return self.env.copy()

    def list_directory(self, path: Optional[str] = None, include_hidden: bool = True, max_entries: int = 5000) -> Dict:
        """
        List directory entries quickly for autocomplete.

        Args:
            path: Directory to list. If None or '.', use current working directory. If relative, resolve against cwd.
            include_hidden: Whether to include entries starting with '.'
            max_entries: Maximum number of entries to return to avoid huge payloads

        Returns:
            Dict with 'success', 'entries' (list of {name, is_dir}), 'truncated' (bool), 'error' keys
        """
        try:
            if not path or path == '.':
                dir_path = self.cwd
            else:
                expanded = os.path.expanduser(path)
                dir_path = expanded if os.path.isabs(expanded) else os.path.join(self.cwd, expanded)
            dir_path = os.path.normpath(dir_path)

            if not os.path.isdir(dir_path):
                return {
                    'success': False,
                    'entries': [],
                    'truncated': False,
                    'error': f"Not a directory: {dir_path}"
                }

            entries = []
            truncated = False
            count = 0
            with os.scandir(dir_path) as it:
                for entry in it:
                    name = entry.name
                    if not include_hidden and name.startswith('.'):
                        continue
                    try:
                        is_dir = entry.is_dir(follow_symlinks=False)
                    except Exception:
                        is_dir = False
                    entries.append({'name': name, 'is_dir': is_dir})
                    count += 1
                    if count >= max_entries:
                        truncated = True
                        break

            return {
                'success': True,
                'entries': entries,
                'truncated': truncated,
                'error': None
            }

        except PermissionError:
            return {
                'success': False,
                'entries': [],
                'truncated': False,
                'error': 'Permission denied'
            }
        except FileNotFoundError:
            return {
                'success': False,
                'entries': [],
                'truncated': False,
                'error': 'Directory not found'
            }
        except Exception as e:
            return {
                'success': False,
                'entries': [],
                'truncated': False,
                'error': str(e)
            }

    def read_file(self, file_path: str) -> Dict:
        """
        Read a file from the remote filesystem and return its contents.
        Limits file size to 1MB for safety.

        Args:
            file_path: Path to the file to read

        Returns:
            Dict with 'success', 'content', 'error', 'size' keys
        """
        try:
            # Convert relative path to absolute
            if not os.path.isabs(file_path):
                file_path = os.path.join(self.cwd, file_path)

            file_path = os.path.normpath(file_path)

            # Check if file exists
            if not os.path.exists(file_path):
                return {
                    'success': False,
                    'content': None,
                    'error': f"File not found: {file_path}",
                    'size': 0
                }

            # Check if it's a file (not a directory)
            if not os.path.isfile(file_path):
                return {
                    'success': False,
                    'content': None,
                    'error': f"Path is not a file: {file_path}",
                    'size': 0
                }

            # Check file size (limit to 1MB)
            file_size = os.path.getsize(file_path)
            max_size = 1024 * 1024  # 1MB

            if file_size > max_size:
                return {
                    'success': False,
                    'content': None,
                    'error': f"File too large: {file_size} bytes (max {max_size} bytes)",
                    'size': file_size
                }

            # Read the file
            with open(file_path, encoding='utf-8', errors='replace') as f:
                content = f.read()

            return {
                'success': True,
                'content': content,
                'error': None,
                'size': file_size
            }

        except UnicodeDecodeError:
            return {
                'success': False,
                'content': None,
                'error': f"File contains binary data or unsupported encoding: {file_path}",
                'size': 0
            }
        except PermissionError:
            return {
                'success': False,
                'content': None,
                'error': f"Permission denied: {file_path}",
                'size': 0
            }
        except Exception as e:
            return {
                'success': False,
                'content': None,
                'error': f"Error reading file: {str(e)}",
                'size': 0
            }

    def write_file(self, file_path: str, content: str) -> Dict:
        """
        Write content to a file on the remote filesystem.

        Args:
            file_path: Path to the file to write
            content: Content to write to the file

        Returns:
            Dict with 'success', 'error' keys
        """
        try:
            # Convert relative path to absolute
            if not os.path.isabs(file_path):
                file_path = os.path.join(self.cwd, file_path)

            file_path = os.path.normpath(file_path)

            # Create directory if it doesn't exist
            dir_path = os.path.dirname(file_path)
            if dir_path and not os.path.exists(dir_path):
                os.makedirs(dir_path, exist_ok=True)

            # Write the file
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)

            return {
                'success': True,
                'error': None
            }

        except PermissionError:
            return {
                'success': False,
                'error': f"Permission denied: {file_path}"
            }
        except Exception as e:
            return {
                'success': False,
                'error': f"Error writing file: {str(e)}"
            }

    def start_interactive_command(self, command: str) -> Dict:
        """
        Start an interactive command that may need stdin.

        Returns:
            Dict with 'success', 'session_id', 'error' keys
        """
        try:
            session_id = str(uuid.uuid4())

            # Use pty for better terminal emulation
            # Set environment variables for unbuffered output
            env = self.env.copy()
            env['PYTHONUNBUFFERED'] = '1'
            env['TERM'] = 'dumb'  # Simple terminal type
            
            # Create a pseudo-terminal
            master_fd, slave_fd = pty.openpty()
            
            process = subprocess.Popen(
                command,
                shell=True,
                stdin=slave_fd,
                stdout=slave_fd,
                stderr=slave_fd,
                text=False,  # Use bytes mode for pty
                cwd=self.cwd,
                env=env,
                preexec_fn=os.setsid,
                close_fds=True
            )
            
            # Close slave fd in parent process
            os.close(slave_fd)

            # Store process info
            self.interactive_processes[session_id] = {
                'process': process,
                'command': command,
                'master_fd': master_fd  # Store the pty master fd
            }

            return {
                'success': True,
                'session_id': session_id,
                'error': None
            }

        except Exception as e:
            return {
                'success': False,
                'session_id': None,
                'error': f"Failed to start interactive command: {str(e)}"
            }

    def send_stdin_data(self, session_id: str, data: str) -> Dict:
        """
        Send data to the stdin of an interactive process.

        Returns:
            Dict with 'success', 'error' keys
        """
        try:
            if session_id not in self.interactive_processes:
                return {
                    'success': False,
                    'error': f"No interactive session found: {session_id}"
                }

            process = self.interactive_processes[session_id]['process']
            master_fd = self.interactive_processes[session_id]['master_fd']

            # Check if process is still running
            if process.poll() is not None:
                return {
                    'success': False,
                    'error': "Process has already terminated"
                }

            # Send data to pty master
            os.write(master_fd, data.encode('utf-8'))

            return {
                'success': True,
                'error': None
            }

        except BrokenPipeError:
            return {
                'success': False,
                'error': "Process stdin closed (process may have terminated)"
            }
        except Exception as e:
            return {
                'success': False,
                'error': f"Error sending stdin data: {str(e)}"
            }

    def read_output_chunk(self, session_id: str, timeout: float = 0.1) -> Dict:
        """
        Read available output from an interactive process (non-blocking).

        Returns:
            Dict with 'success', 'stdout', 'stderr', 'finished', 'returncode' keys
        """
        try:
            if session_id not in self.interactive_processes:
                return {
                    'success': False,
                    'stdout': '',
                    'stderr': '',
                    'finished': True,
                    'returncode': None,
                    'error': f"No interactive session found: {session_id}"
                }

            process = self.interactive_processes[session_id]['process']
            master_fd = self.interactive_processes[session_id]['master_fd']

            # Check if process finished
            returncode = process.poll()
            finished = returncode is not None

            stdout_data = ""
            stderr_data = ""

            # Read from pty master (combines stdout and stderr)
            try:
                import select
                import fcntl
                
                # Set non-blocking mode on master fd
                flags = fcntl.fcntl(master_fd, fcntl.F_GETFL)
                fcntl.fcntl(master_fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)
                
                # Use select to check if data is available
                ready, _, _ = select.select([master_fd], [], [], timeout)
                if ready:
                    try:
                        chunk = os.read(master_fd, 4096)
                        if chunk:
                            # PTY combines stdout/stderr, put it all in stdout
                            stdout_data = chunk.decode('utf-8', errors='replace')
                    except (BlockingIOError, OSError):
                        # No data available or pty closed
                        pass
            except (ImportError, OSError):
                # Fallback - try to read if process finished
                if finished:
                    try:
                        chunk = os.read(master_fd, 4096)
                        stdout_data = chunk.decode('utf-8', errors='replace')
                    except:
                        pass

            return {
                'success': True,
                'stdout': stdout_data,
                'stderr': stderr_data,
                'finished': finished,
                'returncode': returncode,
                'error': None
            }

        except Exception as e:
            return {
                'success': False,
                'stdout': '',
                'stderr': '',
                'finished': True,
                'returncode': None,
                'error': f"Error reading output: {str(e)}"
            }

    def terminate_interactive_command(self, session_id: str) -> Dict:
        """
        Terminate an interactive command and clean up resources.

        Returns:
            Dict with 'success', 'returncode', 'error' keys
        """
        try:
            if session_id not in self.interactive_processes:
                return {
                    'success': True,  # Already cleaned up
                    'returncode': None,
                    'error': None
                }

            process = self.interactive_processes[session_id]['process']

            # Get final returncode
            returncode = process.poll()

            # If process is still running, terminate it
            if returncode is None:
                try:
                    # Try graceful termination first
                    os.killpg(os.getpgid(process.pid), 15)  # SIGTERM = 15
                    process.wait(timeout=5)
                    returncode = process.returncode
                except (ProcessLookupError, subprocess.TimeoutExpired):
                    # Force kill if graceful termination fails
                    try:
                        os.killpg(os.getpgid(process.pid), 9)  # SIGKILL = 9
                        process.wait(timeout=2)
                        returncode = process.returncode
                    except Exception:
                        returncode = -9  # SIGKILL

            # Clean up pty
            master_fd = self.interactive_processes[session_id]['master_fd']
            try:
                os.close(master_fd)
            except Exception:
                pass

            # Remove from active sessions
            del self.interactive_processes[session_id]

            return {
                'success': True,
                'returncode': returncode,
                'error': None
            }

        except Exception as e:
            # Still try to clean up
            try:
                if session_id in self.interactive_processes:
                    del self.interactive_processes[session_id]
            except Exception:
                pass

            return {
                'success': False,
                'returncode': None,
                'error': f"Error terminating process: {str(e)}"
            }

    def cleanup(self) -> None:
        """Clean up any running processes before actor termination."""
        # Clean up regular running process
        with self.process_lock:
            if self.running_process and self.running_process.poll() is None:
                try:
                    os.killpg(os.getpgid(self.running_process.pid), 15)  # SIGTERM = 15
                    self.running_process.wait(timeout=5)
                except (ProcessLookupError, OSError, subprocess.TimeoutExpired):
                    pass

        # Clean up all interactive processes
        for session_id in list(self.interactive_processes.keys()):
            try:
                self.terminate_interactive_command(session_id)
            except Exception:
                pass
