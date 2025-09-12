#!/usr/bin/env python3
"""
Terminal utilities for system-level operations.
Handles PTY configuration, signal delivery, and process management.
"""

import os
import signal
import termios
import fcntl
from typing import List, Optional


def configure_pty_for_signals(pty_slave_fd: int) -> bool:
    """Configure PTY terminal settings for proper signal handling.

    Args:
        pty_slave_fd: File descriptor for the PTY slave

    Returns:
        True if configuration succeeded, False otherwise
    """
    try:
        # Get current terminal attributes
        attrs = termios.tcgetattr(pty_slave_fd)

        # Configure for normal terminal mode with signal generation
        # Local flags
        attrs[3] |= termios.ISIG   # enable signal chars
        attrs[3] |= termios.ICANON # canonical mode for normal line editing
        attrs[3] |= termios.ECHO   # echo so you can see what you type

        # Input flags: ensure we do not strip high-bit and enable UTF-8 if available
        try:
            # Clear ISTRIP so 8-bit characters (UTF-8 bytes) are not stripped
            if hasattr(termios, "ISTRIP"):
                attrs[0] &= ~termios.ISTRIP
            # Set IUTF8 if the platform supports it so the line discipline treats input as UTF-8
            if hasattr(termios, "IUTF8"):
                attrs[0] |= termios.IUTF8
        except Exception:
            pass

        # Control flags: ensure 8-bit chars
        try:
            if hasattr(termios, "CS8"):
                attrs[2] |= termios.CS8
        except Exception:
            pass

        # Set control characters
        attrs[6][termios.VINTR] = ord("\x03")  # Ctrl-C = SIGINT
        attrs[6][termios.VQUIT] = ord("\x1c")  # Ctrl-\ = SIGQUIT
        attrs[6][termios.VSUSP] = ord("\x1a")  # Ctrl-Z = SIGTSTP
        # Make backspace and delete behave commonly
        try:
            if hasattr(termios, "VERASE"):
                attrs[6][termios.VERASE] = ord("\x7f")  # DEL as erase
        except Exception:
            pass

        # Apply the settings
        termios.tcsetattr(pty_slave_fd, termios.TCSANOW, attrs)
        return True
    except Exception:
        return False


def setup_controlling_terminal(pty_slave_fd: int) -> None:
    """Set up the child process as session leader with controlling terminal.

    Args:
        pty_slave_fd: File descriptor for the PTY slave
    """
    # Create new session and set as session leader
    os.setsid()
    # Set the controlling terminal
    fcntl.ioctl(pty_slave_fd, termios.TIOCSCTTY, 0)

    # Ensure signal handling is enabled in the child as well
    try:
        attrs = termios.tcgetattr(pty_slave_fd)
        # Normal mode with signals
        attrs[3] |= termios.ISIG | termios.ICANON | termios.ECHO
        # Input flags: don't strip 8th bit; enable UTF-8 when available
        try:
            if hasattr(termios, "ISTRIP"):
                attrs[0] &= ~termios.ISTRIP
            if hasattr(termios, "IUTF8"):
                attrs[0] |= termios.IUTF8
        except Exception:
            pass
        # Control flags: ensure 8-bit chars
        try:
            if hasattr(termios, "CS8"):
                attrs[2] |= termios.CS8
        except Exception:
            pass
        attrs[6][termios.VINTR] = ord("\x03")  # Ctrl-C = SIGINT
        attrs[6][termios.VQUIT] = ord("\x1c")  # Ctrl-\ = SIGQUIT
        attrs[6][termios.VSUSP] = ord("\x1a")  # Ctrl-Z = SIGTSTP
        termios.tcsetattr(pty_slave_fd, termios.TCSANOW, attrs)
    except Exception:
        pass


def find_child_processes(parent_pid: int) -> List[int]:
    """Find all child processes of a given parent process.

    Args:
        parent_pid: Process ID of the parent process

    Returns:
        List of child process IDs
    """
    child_pids = []
    try:
        for pid_str in os.listdir("/proc"):
            if pid_str.isdigit():
                try:
                    with open(f"/proc/{pid_str}/stat", "r") as f:
                        stat_line = f.read().strip()
                        # Parse stat format: pid (comm) state ppid ...
                        parts = stat_line.split()
                        if len(parts) >= 4:
                            ppid = int(parts[3])
                            if ppid == parent_pid:
                                child_pids.append(int(pid_str))
                except (OSError, ValueError, IndexError):
                    continue
    except OSError:
        pass
    return child_pids


def send_signal_to_children(parent_pid: int, sig: int) -> bool:
    """Send a signal to all child processes of a parent.

    Args:
        parent_pid: Process ID of the parent process
        sig: Signal number to send (e.g., signal.SIGTERM)

    Returns:
        True if at least one signal was sent successfully
    """
    child_pids = find_child_processes(parent_pid)
    success = False

    for pid in child_pids:
        try:
            os.kill(pid, sig)
            success = True
        except Exception:
            pass

    return success


def handle_ctrl_c_signal(shell_pid: int) -> bool:
    """Handle Ctrl-C by terminating child processes gracefully.

    This intentionally maps Ctrl-C to SIGTERM for better compatibility with
    certain workloads (e.g., sleep) where SIGINT may be ignored.

    Args:
        shell_pid: Process ID of the shell process

    Returns:
        True if signal was handled (sent to children), False if should use PTY default
    """
    return send_signal_to_children(shell_pid, signal.SIGTERM)
