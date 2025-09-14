#!/usr/bin/env python3
"""
File streaming Ray actor for pulling runtime_env working_dir contents over Ray Client.

This actor is intended to be created with runtime_env={"working_dir": <gcs_uri>} so that
Ray unpacks the package for the actor's process. It then archives the working directory
and streams it back to the client in chunks.
"""

import os
import tempfile
import hashlib
import tarfile
from typing import Dict, Optional

import ray


@ray.remote(num_gpus=0, num_cpus=0)
class FileStreamerActor:
    def __init__(self):
        self._archive_path: Optional[str] = None
        self._archive_size: int = 0
        self._archive_sha256: Optional[str] = None
        self._root_basename: Optional[str] = None

    def prepare_archive(self) -> Dict:
        """Create a tar.gz archive of the actor's working directory and return metadata."""
        # Actor's current working directory is the unpacked runtime_env working_dir
        root_dir = os.getcwd()
        self._root_basename = os.path.basename(root_dir.rstrip(os.sep)) or "package"

        fd, tmp_path = tempfile.mkstemp(prefix="rayssh_pull_", suffix=".tar.gz")
        os.close(fd)

        # Create tar.gz with a single top-level folder (root_basename)
        with tarfile.open(tmp_path, "w:gz") as tf:
            tf.add(root_dir, arcname=self._root_basename)

        # Compute size and sha256
        self._archive_path = tmp_path
        self._archive_size = os.path.getsize(tmp_path)
        h = hashlib.sha256()
        with open(tmp_path, "rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                h.update(chunk)
        self._archive_sha256 = h.hexdigest()

        return {
            "archive_path": self._archive_path,
            "size": self._archive_size,
            "sha256": self._archive_sha256,
            "root_basename": self._root_basename,
        }

    def read_chunk(self, offset: int, length: int) -> bytes:
        """Read a chunk from the prepared archive."""
        if not self._archive_path:
            raise RuntimeError("Archive not prepared")
        with open(self._archive_path, "rb") as f:
            f.seek(offset)
            return f.read(length)

    def cleanup(self) -> bool:
        try:
            if self._archive_path and os.path.exists(self._archive_path):
                os.remove(self._archive_path)
        except Exception:
            pass
        self._archive_path = None
        self._archive_size = 0
        self._archive_sha256 = None
        self._root_basename = None
        return True
