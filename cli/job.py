#!/usr/bin/env python3
"""
Job submission utilities for RaySSH CLI.
"""

import os
import getpass
import datetime
import subprocess
import sys

from utils import ensure_ray_initialized
from .job_utils import (
    validate_and_detect_interpreter,
    get_runtime_env_options,
    parse_n_nodes_from_env,
    parse_job_entrypoint_gpus,
    parse_per_node_gpus_for_multinode,
    get_master_port_default,
)
from .torchrun_orchestrator import ORCHESTRATOR_SCRIPT


def _generate_submission_id(kind: str | None = None) -> str:
    """Generate a concise, mostly-unique Ray job submission id.

    Format: {username}_[{kind}_]{yymmddHHMMSS}
    """
    try:
        username = (
            os.environ.get("USER") or os.environ.get("LOGNAME") or getpass.getuser()
        )
    except Exception:
        username = "rayuser"
    ts = datetime.datetime.now().strftime("%y%m%d%H%M%S")
    if kind:
        return f"{username}_{kind}_{ts}"
    return f"{username}_{ts}"


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

        # Validate and detect interpreter
        interpreter, err = validate_and_detect_interpreter(file_path)
        if err:
            print(err, file=sys.stderr)
            return 1

        # Prepare working dir and runtime env options
        working_dir_opt, runtime_env_file, runtime_env_present = (
            get_runtime_env_options()
        )

        # Parse n_nodes for torchrun-like multi-node execution (only for file jobs)
        try:
            n_nodes = parse_n_nodes_from_env()
        except ValueError as e:
            print(str(e), file=sys.stderr)
            return 1

        # Parse GPU envs
        entrypoint_num_gpus_arg = None
        gpu_str = None
        if n_nodes is None:
            try:
                entrypoint_num_gpus_arg, gpu_str = parse_job_entrypoint_gpus()
            except ValueError as e:
                print(str(e), file=sys.stderr)
                return 1

        # Build Ray job submit command
        submission_id = _generate_submission_id(interpreter)
        cmd = [
            "ray",
            "job",
            "submit",
            "--entrypoint-num-cpus=1",
            f"--submission-id={submission_id}",
            working_dir_opt,
        ]

        if entrypoint_num_gpus_arg:
            cmd.append(entrypoint_num_gpus_arg)

        if runtime_env_present:
            cmd.append(f"--runtime-env={runtime_env_file}")

        cmd.append("--")

        if no_wait:
            # Insert no-wait just before entrypoint
            cmd.insert(cmd.index("--"), "--no-wait")

        if n_nodes is None:
            # Legacy: single process execution inside one Ray job
            cmd.extend([interpreter, file_path])
        else:
            # Multi-node orchestrator: one actor per node, passing torchrun-like envs
            # Determine per-node GPU setting
            per_node_gpu_str = None
            warn_msg = None
            try:
                per_node_gpu_str, warn_msg = parse_per_node_gpus_for_multinode()
            except ValueError as e:
                print(str(e), file=sys.stderr)
                return 1
            if warn_msg:
                print(f"⚠️  {warn_msg}", file=sys.stderr)

            # Default master port if not provided in env
            master_port_env = get_master_port_default()

            orchestrator_script = ORCHESTRATOR_SCRIPT

            # Build entrypoint for orchestrator
            cmd.extend(
                [
                    "python",
                    "-c",
                    orchestrator_script,
                    str(n_nodes),
                    interpreter,
                    file_path,
                    per_node_gpu_str or "",
                    str(master_port_env),
                ]
            )

        # Print concise context
        if n_nodes is None:
            print(f"🚀 RaySSH: Submitting {interpreter} job: {file_path}")
        else:
            print(
                f"🚀 RaySSH: Submitting multi-node {interpreter} job: {file_path} (nodes={n_nodes})"
            )
        print(f"📦 Working dir: .")
        if runtime_env_present:
            print(f"🧩 Runtime env: ./{runtime_env_file}")
        else:
            print(f"🧩 Runtime env: remote (create runtime_env.yaml to customize)")
        if n_nodes is None:
            if gpu_str is not None:
                print(f"🎛️ GPUs: {gpu_str}")
        else:
            if per_node_gpu_env is not None or gpu_env is not None:
                print(
                    f"🎛️ GPUs per node: {per_node_gpu_str if 'per_node_gpu_str' in locals() and per_node_gpu_str else '0'}"
                )
            print(
                f"🔌 Torchrun envs: N_NODES, NODE_RANK, MASTER_ADDRESS, MASTER_PORT={os.environ.get('MASTER_PORT') or '29500'}"
            )
        print(f"📋 Command: {' '.join(cmd)}")
        print()

        # Execute the ray job submit command
        try:
            result = subprocess.run(cmd, cwd=".", env=os.environ)
            return result.returncode
        except KeyboardInterrupt:
            # Graceful interrupt: avoid Python traceback and provide a clean line
            print("\n⏸️ Aborted following job output.")
            print(
                "If the job is already up and running, you can continue following its output:"
            )
            print(f"ray job logs -f {submission_id}")
            print(f"Or stop it:")
            print(f"ray job stop {submission_id}")
            # 130 is conventional exit code for SIGINT
            return 130

    except Exception as e:
        print(f"Error submitting job: {e}", file=sys.stderr)
        return 1


def submit_shell_command(command: str) -> int:
    """
    Submit an arbitrary shell command as a Ray job and stream logs.
    Respects n_gpus from environment.
    """
    try:
        # Ensure Ray is initialized; use remote if configured
        ensure_ray_initialized()

        working_dir_opt = "--working-dir=."

        # Parse GPUs from environment variable (supports 'n_gpus' or 'N_GPUS')
        gpu_env = os.environ.get("n_gpus") or os.environ.get("N_GPUS")
        entrypoint_num_gpus_arg = None
        gpu_str = None
        if gpu_env is not None and gpu_env != "":
            try:
                gpu_count = float(gpu_env)
                if gpu_count < 0:
                    raise ValueError
                gpu_str = (
                    str(int(gpu_count)) if gpu_count.is_integer() else str(gpu_count)
                )
                entrypoint_num_gpus_arg = f"--entrypoint-num-gpus={gpu_str}"
            except Exception:
                print(
                    f"Error: Invalid n_gpus value '{gpu_env}'. Must be a number >= 0.",
                    file=sys.stderr,
                )
                return 1

        # Build job submit command to run via bash -lc "<command>"
        submission_id = _generate_submission_id("cmd")
        cmd = [
            "ray",
            "job",
            "submit",
            "--entrypoint-num-cpus=1",
            working_dir_opt,
            f"--submission-id={submission_id}",
        ]
        if entrypoint_num_gpus_arg:
            cmd.append(entrypoint_num_gpus_arg)

        # Prefer runtime_env.yaml if present
        runtime_env_candidates = ["runtime_env.yaml"]
        runtime_env_file = next(
            (f for f in runtime_env_candidates if os.path.isfile(f)), None
        )
        if runtime_env_file:
            cmd.append(f"--runtime-env={runtime_env_file}")

        # Pass the command directly to bash -lc (already reconstructed by caller)
        cmd += ["--", "bash", "-lc", command]

        print(f"🚀 RaySSH: Submitting command job")
        print(f"   💬 {command}")
        if gpu_str is not None:
            print(f"   🎛️ GPUs: {gpu_str}")
        print(f"   📋 {' '.join(cmd)}")
        print()

        try:
            result = subprocess.run(cmd, cwd=".")
            return result.returncode
        except KeyboardInterrupt:
            print(
                "\n⏸️ Aborted following job output. The Ray job submission command was interrupted."
            )
            print(
                "If the job is already up and running, you can continue following its output:"
            )
            print(f"ray job logs -f {submission_id}")
            print(f"Or stop it:")
            print(f"ray job stop {submission_id}")
            return 130
    except Exception as e:
        print(f"Error submitting command job: {e}", file=sys.stderr)
        return 1
