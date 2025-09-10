"""
CLI package for RaySSH
Provides command-line interface utilities and node management.
"""

from .node import (
    get_ordered_nodes,
    get_node_by_index,
    get_random_worker_node,
    print_nodes_table,
    interactive_node_selector,
)
from .job import submit_file_job
from .lab import handle_lab_command
from .code import handle_code_command
from .debug import handle_debug_command
from .job import submit_shell_command
from .cursor import handle_tell_cursor_command
from .transfer import handle_push_command, handle_pull_command

__all__ = [
    "get_ordered_nodes",
    "get_node_by_index",
    "get_random_worker_node",
    "print_nodes_table",
    "interactive_node_selector",
    "submit_file_job",
    "handle_lab_command",
    "handle_code_command",
    "handle_debug_command",
    "submit_shell_command",
    "handle_tell_cursor_command",
    "handle_push_command",
    "handle_pull_command",
]
