#!/usr/bin/env python3
"""
Push/Pull utilities for RaySSH using Ray runtime_env working_dir packages.
"""

import os
import sys
import shutil
import tempfile
from typing import List, Dict

import ray

from utils import ensure_ray_initialized, fetch_cluster_nodes, get_head_node_id


@ray.remote(num_cpus=0)
def _pack_current_workdir_zip() -> Dict:
    """Pack the current working directory into a zip and return it in chunks.

    Assumes this task is started with runtime_env working_dir pointing to the
    package URI, so os.getcwd() is the unpacked package directory.
    """
    import os as _os
    import tempfile as _tempfile
    import shutil as _shutil

    cwd = _os.getcwd()
    base_name = _os.path.basename(cwd.rstrip("/")) or "package"

    tmpdir = _tempfile.mkdtemp()
    archive_base = _os.path.join(tmpdir, base_name)
    # Create zip of entire cwd
    archive_path = _shutil.make_archive(archive_base, "zip", root_dir=cwd)

    # Stream as chunks
    chunks: list[bytes] = []
    chunk_size = 8 * 1024 * 1024  # 8MB
    with open(archive_path, "rb") as f:
        while True:
            data = f.read(chunk_size)
            if not data:
                break
            chunks.append(data)

    return {"filename": _os.path.basename(archive_path), "chunks": chunks}


def _package_dir_and_get_gcs_uri(path: str) -> str:
    """Initialize Ray with working_dir=path and return the produced gcs:// package URI."""
    # Ensure absolute
    abs_path = os.path.abspath(path)
    if not os.path.exists(abs_path):
        raise FileNotFoundError(f"Path not found: {abs_path}")
    if not os.path.isdir(abs_path):
        raise ValueError("_package_dir_and_get_gcs_uri expects a directory")

    # Initialize Ray Client with working_dir
    ensure_ray_initialized(ray_address=os.environ.get("RAY_ADDRESS"), working_dir=abs_path)

    # Get runtime env to fetch the uploaded package URI
    ctx = ray.get_runtime_context()
    re = ctx.runtime_env
    uri = re.get("working_dir")
    if not uri or not str(uri).startswith("gcs://"):
        raise RuntimeError("Could not obtain gcs:// working_dir package URI from Ray context")
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
        print("Usage: rayssh pull <gcs://_ray_pkg_xxx.zip>\n"
        "You can get a gcs://_ray_pkg_xxx.zip URI by:\n"
        "1. Uploading content using 'rayssh push <path>' command\n"
        "2. Inspecting on Ray dashboard any rayssh / Ray operations that involves uploading a working dir, click its `runtime environment` link and find the working_dir URI\n", file=sys.stderr)
        return 1

    uri = args[0]
    if not (uri.startswith("gcs://") and uri.endswith(".zip")):
        print("Error: pull expects a gcs://_ray_pkg_xxx.zip URI. You can get a gcs://_ray_pkg_xxx.zip URI by:\n"
        "1. Uploading file/dir using 'rayssh push <path>' command\n"
        "2. Inspecting on Ray dashboard any previous rayssh / Ray operations that involves uploading a working dir, click its `runtime environment` link and find the working_dir URI\n", file=sys.stderr)
        return 1

    try:
        # Require Ray Client to run a remote packing task
        ray_addr = os.environ.get("RAY_ADDRESS")
        if not ray_addr:
            print("Error: RAY_ADDRESS must be set to pull via Ray cluster", file=sys.stderr)
            return 1

        # Make sure Ray Client is initialized first (no local working_dir upload here)
        ensure_ray_initialized(ray_address=ray_addr, working_dir=None)

        # Place task on head node (soft), with runtime_env to fetch the package
        try:
            nodes, head_node_id = fetch_cluster_nodes()
        except Exception:
            nodes, head_node_id = [], None

        scheduling_strategy = None
        try:
            if head_node_id:
                scheduling_strategy = ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
                    node_id=head_node_id, soft=True
                )
        except Exception:
            scheduling_strategy = None

        options_kwargs = {"num_cpus": 0, "runtime_env": {"working_dir": uri}}
        if scheduling_strategy is not None:
            options_kwargs["scheduling_strategy"] = scheduling_strategy

        obj = _pack_current_workdir_zip.options(**options_kwargs).remote()
        result = ray.get(obj)
        filename = result.get("filename") or os.path.basename(uri)
        chunks = result.get("chunks") or []

        # Write zip locally
        zip_path = os.path.join(os.getcwd(), filename)
        with open(zip_path, "wb") as wf:
            for c in chunks:
                wf.write(c)

        # Unpack into CWD
        try:
            shutil.unpack_archive(zip_path, extract_dir=os.getcwd(), format="zip")
            print("✅ Pulled and unpacked:")
            print(f"- Zip: {zip_path}")
        except Exception as ue:
            print(f"⚠️  Unpack failed, leaving zip in place: {zip_path} ({ue})")

        return 0
    except Exception as e:
        print(f"Error during pull: {e}", file=sys.stderr)
        return 1


