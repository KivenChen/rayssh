#!/usr/bin/env python3
"""
Push/Pull utilities for RaySSH using Ray runtime_env working_dir packages.
"""

import os
import sys
import shutil
import tempfile
import hashlib
from typing import List

import ray

from utils import ensure_ray_initialized, fetch_cluster_nodes, get_head_node_id
from agent.file_streamer import FileStreamerActor


def _package_dir_and_get_gcs_uri(path: str) -> str:
    """Initialize Ray with working_dir=path and return the produced gcs:// package URI."""
    # Ensure absolute
    abs_path = os.path.abspath(path)
    if not os.path.exists(abs_path):
        raise FileNotFoundError(f"Path not found: {abs_path}")
    if not os.path.isdir(abs_path):
        raise ValueError("_package_dir_and_get_gcs_uri expects a directory")

    # Initialize Ray Client with working_dir
    ensure_ray_initialized(
        ray_address=os.environ.get("RAY_ADDRESS"), working_dir=abs_path
    )

    # Get runtime env to fetch the uploaded package URI
    ctx = ray.get_runtime_context()
    re = ctx.runtime_env
    uri = re.get("working_dir")
    if not uri or not str(uri).startswith("gcs://"):
        raise RuntimeError(
            "Could not obtain gcs:// working_dir package URI from Ray context"
        )
    return uri


def handle_push_command(argv: List[str]) -> int:
    """Handle 'rayssh push <path>' - produce a gcs:// package URI for the path."""
    args = argv
    if len(argv) >= 1 and argv[0] == "push":
        args = argv[1:]

    if not args:
        print("Usage: rayssh push <file-or-dir>", file=sys.stderr)
        return 1

    src = args[0]
    if not os.path.exists(src):
        print(f"Error: Path not found: {src}", file=sys.stderr)
        return 1

    try:
        if os.path.isdir(src):
            uri = _package_dir_and_get_gcs_uri(src)
        else:
            # For a single file, stage into a temp dir with enclosing folder
            base = os.path.basename(src)
            enclosing = os.path.splitext(base)[0] or "package"
            with tempfile.TemporaryDirectory() as td:
                pkg_root = os.path.join(td, enclosing)
                os.makedirs(pkg_root, exist_ok=True)
                shutil.copy2(src, os.path.join(pkg_root, base))
                uri = _package_dir_and_get_gcs_uri(pkg_root)

        print("📦 Package uploaded:")
        print(uri)
        return 0
    except Exception as e:
        print(f"Error during push: {e}", file=sys.stderr)
        return 1


def handle_pull_command(argv: List[str]) -> int:
    """Handle 'rayssh pull <gcs_uri>' - download/unpack package into current directory."""
    args = argv
    if len(argv) >= 1 and argv[0] == "pull":
        args = argv[1:]

    if not args:
        print(
            "Usage: rayssh pull <gcs://_ray_pkg_xxx.zip>\n"
            "You can get a gcs://_ray_pkg_xxx.zip URI by:\n"
            "1. Uploading content using 'rayssh push <path>' command\n"
            "2. Inspecting on Ray dashboard any rayssh / Ray operations that involves uploading a working dir, click its `runtime environment` link and find the working_dir URI\n",
            file=sys.stderr,
        )
        return 1

    uri = args[0]
    if not (uri.startswith("gcs://") and uri.endswith(".zip")):
        print(
            "Error: pull expects a gcs://_ray_pkg_xxx.zip URI. You can get a gcs://_ray_pkg_xxx.zip URI by:\n"
            "1. Uploading file/dir using 'rayssh push <path>' command\n"
            "2. Inspecting on Ray dashboard any previous rayssh / Ray operations that involves uploading a working dir, click its `runtime environment` link and find the working_dir URI\n",
            file=sys.stderr,
        )
        return 1

    try:
        # Ensure Ray client is initialized (no local cwd upload)
        ensure_ray_initialized(
            ray_address=os.environ.get("RAY_ADDRESS"), working_dir=None
        )

        # Locate head node for placement
        nodes, head_node_id = fetch_cluster_nodes()
        if not head_node_id:
            head_node_id = get_head_node_id()
        if not head_node_id:
            print("Error: Could not determine Ray head node", file=sys.stderr)
            return 1

        # Create a FileStreamerActor on the head node with runtime_env to unpack the package
        streamer = FileStreamerActor.options(
            runtime_env={"working_dir": uri},
            scheduling_strategy=ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
                node_id=head_node_id, soft=False
            ),
            lifetime="detached",
            name=None,
            namespace="rayssh",
        ).remote()

        # Prepare archive remotely
        meta = ray.get(streamer.prepare_archive.remote())
        size = int(meta.get("size", 0))
        sha256 = str(meta.get("sha256", ""))
        root_basename = meta.get("root_basename") or "package"

        # Receive chunks and write to local tar.gz
        pkg_name = os.path.splitext(os.path.basename(uri))[0]
        local_archive = os.path.join(os.getcwd(), f"{pkg_name}.tar.gz")
        offset = 0
        chunk_size = 2 * 1024 * 1024
        h = hashlib.sha256()
        with open(local_archive, "wb") as out:
            while offset < size:
                to_read = min(chunk_size, size - offset)
                data = ray.get(streamer.read_chunk.remote(offset, to_read))
                if not data:
                    break
                out.write(data)
                h.update(data)
                offset += len(data)

        # Verify hash
        if sha256 and h.hexdigest() != sha256:
            print("Error: SHA256 mismatch after download", file=sys.stderr)
            try:
                ray.get(streamer.cleanup.remote())
            except Exception:
                pass
            try:
                ray.kill(streamer)
            except Exception:
                pass
            return 1

        # Extract into CWD
        import tarfile as _tarfile

        with _tarfile.open(local_archive, "r:gz") as tf:
            tf.extractall(os.getcwd())

        # Cleanup remote archive and actor
        try:
            ray.get(streamer.cleanup.remote())
        except Exception:
            pass
        try:
            ray.kill(streamer)
        except Exception:
            pass

        print("✅ Pulled package contents:")
        print(f"- Source: {uri}")
        print(f"- Saved:  {local_archive}")
        print(f"- Extracted folder: {os.path.join(os.getcwd(), root_basename)}")
        return 0
    except Exception as e:
        print(f"Error during pull: {e}", file=sys.stderr)
        return 1
