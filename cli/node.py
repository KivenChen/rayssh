#!/usr/bin/env python3
"""
Node management utilities for RaySSH CLI.
"""

import os
import random
import sys

from utils import ensure_ray_initialized, fetch_cluster_nodes_via_state


def get_ordered_nodes():
    """
    Get Ray nodes in the same order as displayed in --ls table.
    Returns (nodes, head_node_index) where nodes is list of alive nodes
    and head_node_index is the index of the head node.
    """
    nodes, head_node_id = fetch_cluster_nodes_via_state()
    if not nodes:
        return [], -1

    # Compute head index in this list (fallback to 0 if not found)
    head_node_index = 0
    if head_node_id:
        for i, node in enumerate(nodes):
            if node.get("NodeID") == head_node_id:
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
        raise ValueError(
            f"Node index -{index} is out of range. Available: -0 to -{len(non_head_nodes)}"
        )

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
        raise ValueError(
            "Only head node available. Use 'rayssh -0' to connect to head node, or 'rayssh -l' to see all nodes."
        )

    # Randomly select a worker node
    selected_node = random.choice(worker_nodes)
    return selected_node


def print_nodes_table():
    """Print a table of available Ray nodes."""
    try:
        # Ensure Ray is initialized for node listing
        ensure_ray_initialized()

        nodes, head_node_id = fetch_cluster_nodes_via_state()
        if not nodes:
            print("🚫 No Ray nodes found in the cluster.")
            return 0

        print(f"🌐 Ray Cluster Nodes ({len(nodes)} alive)")
        print("=" * 85)

        # Header
        print(
            f"{'📍 Node':<12} {'🌍 IP Address':<16} {'🆔 ID':<8} {'🖥️  CPU':<12} {'🎮 GPU':<12} {'💾 Memory':<12}"
        )
        print("-" * 85)

        # Print rows
        for i, node in enumerate(nodes, 1):
            node_ip = node.get("NodeManagerAddress", "N/A") or "N/A"
            full_node_id = node.get("NodeID", "N/A") or "N/A"
            node_id_short = f"{full_node_id[:6]}..." if full_node_id != "N/A" else "N/A"

            is_head_node = full_node_id == head_node_id
            node_label = f"{'👑 Head' if is_head_node else str(i)}"

            resources = node.get("Resources", {}) or {}
            cpu_total = int(resources.get("CPU", 0)) if resources.get("CPU", 0) else 0
            gpu_total = int(resources.get("GPU", 0)) if resources.get("GPU", 0) else 0

            # memory reported may be bytes; format to GB
            memory_val = resources.get("memory", 0) or 0
            try:
                memory_gb = (
                    f"{(float(memory_val) / (1024**3)):.1f}GB"
                    if float(memory_val) > 0
                    else "0GB"
                )
            except Exception:
                memory_gb = "0GB"

            print(
                f"{node_label:<12} {node_ip:<16} {node_id_short:<8} {cpu_total:<12} {gpu_total:<12} {memory_gb:<12}"
            )

            # Special resources
            special_resources = []
            for key, value in sorted(resources.items()):
                if key not in [
                    "CPU",
                    "GPU",
                    "memory",
                    "object_store_memory",
                ] and not key.startswith("node:"):
                    if "accelerator" in key.lower() or "tpu" in key.lower():
                        special_resources.append(
                            f"⚡ {key}: {int(value) if isinstance(value, float) and value.is_integer() else value}"
                        )
                    else:
                        special_resources.append(
                            f"🔧 {key}: {int(value) if isinstance(value, float) and value.is_integer() else value}"
                        )
            if special_resources:
                print(f"{'':12} {'└─':16} {', '.join(special_resources)}")

        print("=" * 85)
        print(
            "💡 Use: rayssh <ip_address> or rayssh <node_id_prefix> or rayssh -<index> to connect"
        )
        print("👑 Head node is -0, first non-head is -1, second non-head is -2, etc.")
        print(
            "🎲 Use 'rayssh' (no args) for random worker, 'rayssh -l' for interactive, 'rayssh --ls' for this table"
        )

    except Exception as e:
        print(f"❌ Error listing nodes: {e}", file=sys.stderr)
        return 1

    return 0


def interactive_node_selector():
    """
    Display an interactive node selection screen.
    Returns the selected node's IP address or None if cancelled.
    """
    try:
        # Ensure Ray is initialized for node selection
        ensure_ray_initialized()

        # Get nodes in the same order as --ls table
        nodes, head_node_index = get_ordered_nodes()

        if not nodes:
            print("🚫 No Ray nodes found in the cluster.")
            return None

        # Create display list with proper indexing
        display_nodes = []
        for i, node in enumerate(nodes):
            node_ip = node.get("NodeManagerAddress", "N/A")
            node_id = node.get("NodeID", "N/A")[:6]
            is_head = i == head_node_index

            # Get resources for display
            resources = node.get("Resources", {})
            cpu = int(resources.get("CPU", 0)) if resources.get("CPU", 0) else 0
            gpu = int(resources.get("GPU", 0)) if resources.get("GPU", 0) else 0
            memory_gb = (
                resources.get("memory", 0) / (1024**3) if resources.get("memory") else 0
            )

            display_nodes.append(
                {
                    "node": node,
                    "ip": node_ip,
                    "id": node_id,
                    "is_head": is_head,
                    "cpu": cpu,
                    "gpu": gpu,
                    "memory": f"{memory_gb:.1f}GB",
                    "index": 0
                    if is_head
                    else len(
                        [n for j, n in enumerate(nodes[:i]) if j != head_node_index]
                    )
                    + 1,
                }
            )

        # Simple numbered selection
        os.system("clear")
        print("🎯 RaySSH: Node Selection")
        print("=" * 50)
        print()

        # Display nodes as a numbered list with 0-based indexing matching command line
        print("    #    Type     IP Address       ID       CPU    GPU    Memory")
        print("-" * 60)

        for i, display_node in enumerate(display_nodes):
            # Use 0 for head node, then 1, 2, 3... for non-head nodes
            if display_node["is_head"]:
                node_number = 0
                node_type = "HEAD"
            else:
                node_number = display_node["index"]
                node_type = f"-{display_node['index']}"

            print(
                f"    {node_number:<4} {node_type:<8} {display_node['ip']:<16} {display_node['id']:<8} {display_node['cpu']:<6} {display_node['gpu']:<6} {display_node['memory']:<8}"
            )

        print()
        print("=" * 50)
        print("🔢 Enter node number to connect (0=head, 1+=non-head, 'q' to cancel):")

        try:
            choice = input("> ").strip()
            if choice.lower() in ["q", "quit", "exit"]:
                return None
            if choice == "":
                return None

            node_num = int(choice)

            # Find the node with the matching number
            selected_node = None
            for display_node in display_nodes:
                if display_node["is_head"] and node_num == 0:
                    selected_node = display_node
                    break
                elif not display_node["is_head"] and node_num == display_node["index"]:
                    selected_node = display_node
                    break

            if selected_node:
                print(f"🚀 Connecting to {selected_node['ip']}...")
                return selected_node["ip"]
            else:
                # Show available numbers for error message
                available_nums = []
                for display_node in display_nodes:
                    if display_node["is_head"]:
                        available_nums.append("0")
                    else:
                        available_nums.append(str(display_node["index"]))
                print(
                    f"❌ Invalid selection. Available: {', '.join(available_nums)} or 'q' to cancel."
                )
                return None

        except (ValueError, KeyboardInterrupt):
            return None

    except Exception as e:
        print(f"❌ Error in node selector: {e}", file=sys.stderr)
        return None
