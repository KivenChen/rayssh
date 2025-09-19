#!/usr/bin/env python3
"""
SyncClient: watches a local directory and syncs text files (<=1MB) to remote via WebSocket.
"""

import asyncio
import base64
import json
import os
from typing import Iterable, List, Optional, Set, Tuple

from watchfiles import awatch, Change


DEFAULT_IGNORES: Tuple[str, ...] = (
    ".git",
    "__pycache__",
    ".venv",
    "node_modules",
    ".DS_Store",
    ".ipynb_checkpoints",
    ".cache",
    ".vscode",
    ".idea",
    ".pytest_cache",
    ".ruff_cache",
    ".mypy_cache",
)


class SyncClient:
    def __init__(self, local_root: str, websocket=None, session_id=None):
        self.local_root = os.path.abspath(local_root)
        self.websocket = websocket
        self.session_id = session_id
        self.running = False
        self.shutdown_requested = False
        self._external_websocket = websocket is not None

    async def connect(self, host: str = None, port: int = None) -> None:
        # If using external websocket, no setup needed
        if self._external_websocket:
            return

        # Original connection logic for standalone use
        if not host or not port:
            raise ValueError("host and port required when not using external websocket")

        import websockets

        uri = f"ws://{host}:{port}/sync"
        self.websocket = await websockets.connect(
            uri, ping_interval=20, ping_timeout=10
        )
        # Note: No hello message needed when using session-based routing

    def _is_ignored(self, path: str) -> bool:
        rel = os.path.relpath(path, self.local_root)
        if rel.startswith(".."):
            return True
        parts = rel.split(os.sep)
        for p in parts:
            if not p:
                continue
            if p.startswith(".") and p not in (".env",):
                return True
            if p in DEFAULT_IGNORES:
                return True
        return False

    def _is_text_and_small(self, full_path: str) -> bool:
        try:
            if not os.path.isfile(full_path):
                return False
            size = os.path.getsize(full_path)
            if size > 1024 * 1024:
                return False
            with open(full_path, "rb") as f:
                chunk = f.read(4096)
            # Heuristic: consider text if no NULs and decodes in utf-8/latin-1
            if b"\x00" in chunk:
                return False
            try:
                chunk.decode("utf-8")
            except UnicodeDecodeError:
                try:
                    chunk.decode("latin-1")
                except UnicodeDecodeError:
                    return False
            return True
        except Exception:
            return False

    def _relpath(self, full_path: str) -> Optional[str]:
        try:
            rel = os.path.relpath(full_path, self.local_root)
            if rel.startswith(".."):
                return None
            return rel.replace("\\", "/")
        except Exception:
            return None

    async def _send_json(self, obj: dict) -> None:
        if not self.websocket:
            return
        # Add session_id to all sync messages when available
        if self.session_id:
            obj["session_id"] = self.session_id
        await self.websocket.send(json.dumps(obj))

    async def _send_file_put(self, full_path: str) -> None:
        rel = self._relpath(full_path)
        if not rel:
            return
        try:
            stat = os.stat(full_path)
            with open(full_path, "rb") as f:
                data = f.read()
            payload = base64.b64encode(data).decode("ascii")
            await self._send_json(
                {
                    "type": "sync_put",
                    "relpath": rel,
                    "size": len(data),
                    "mtime": stat.st_mtime,
                    "mode": oct(stat.st_mode & 0o777)[2:],
                    "encoding": "base64",
                    "content": payload,
                }
            )
        except FileNotFoundError:
            # File disappeared; skip
            return
        except Exception:
            return

    async def _send_delete(self, full_path: str) -> None:
        rel = self._relpath(full_path)
        if not rel:
            return
        await self._send_json({"type": "sync_del", "relpath": rel})

    async def _send_move(self, src_path: str, dst_path: str) -> None:
        src_rel = self._relpath(src_path)
        dst_rel = self._relpath(dst_path)
        if not src_rel or not dst_rel:
            return
        await self._send_json({"type": "sync_move", "src": src_rel, "dst": dst_rel})

    async def run(self, host: str = None, port: int = None) -> None:
        if not self._external_websocket:
            await self.connect(host, port)

        self.running = True
        try:
            async for changes in awatch(self.local_root, stop_event=None, debounce=300):
                if not self.running or self.shutdown_requested or not self.websocket:
                    break
                # Coalesce events by path, prefer moves
                moves: List[Tuple[str, str]] = []
                puts: Set[str] = set()
                deletes: Set[str] = set()
                for change, path in changes:
                    if self._is_ignored(path):
                        continue
                    if change == Change.modified or change == Change.added:
                        if self._is_text_and_small(path):
                            puts.add(path)
                    elif change == Change.deleted:
                        deletes.add(path)
                # Send deletes first to avoid conflicts
                for p in list(deletes):
                    try:
                        await self._send_delete(p)
                    except Exception:
                        pass
                # Then puts
                for p in list(puts):
                    try:
                        await self._send_file_put(p)
                    except Exception:
                        pass
        finally:
            await self.cleanup()

    async def cleanup(self) -> None:
        self.running = False
        self.shutdown_requested = True

        # Only close websocket if we own it (not external)
        if self.websocket and not self._external_websocket:
            try:
                await self.websocket.close()
            except Exception:
                pass
            self.websocket = None
        elif self._external_websocket:
            # For external websocket, just clear our reference
            self.websocket = None
