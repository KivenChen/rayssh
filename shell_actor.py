import os
import signal
import subprocess
import threading
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

    def get_node_info(self) -> Dict:
        """Get information about the current node."""
        return {
            'hostname': os.uname().nodename,
            'cwd': self.cwd,
            'pid': os.getpid(),
            'user': os.environ.get('USER', 'unknown')
        }

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
            with self.process_lock:
                # Start the process
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

                try:
                    # Wait for completion with timeout
                    stdout, stderr = self.running_process.communicate(timeout=timeout)
                    returncode = self.running_process.returncode

                except subprocess.TimeoutExpired:
                    # Kill the process group on timeout
                    os.killpg(os.getpgid(self.running_process.pid), signal.SIGTERM)
                    stdout, stderr = self.running_process.communicate()
                    returncode = -15  # SIGTERM
                    stderr += f"\nCommand timed out after {timeout} seconds\n"

                finally:
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
                    os.killpg(os.getpgid(self.running_process.pid), signal.SIGINT)
                    return True
                except (ProcessLookupError, OSError):
                    # Process already terminated
                    pass
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
                    os.killpg(os.getpgid(self.running_process.pid), signal.SIGTSTP)
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

    def cleanup(self) -> None:
        """Clean up any running processes before actor termination."""
        with self.process_lock:
            if self.running_process and self.running_process.poll() is None:
                try:
                    os.killpg(os.getpgid(self.running_process.pid), signal.SIGTERM)
                    self.running_process.wait(timeout=5)
                except (ProcessLookupError, OSError, subprocess.TimeoutExpired):
                    pass
