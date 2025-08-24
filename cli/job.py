#!/usr/bin/env python3
"""
Job submission utilities for RaySSH CLI.
"""

import os
import subprocess
import sys

from utils import ensure_ray_initialized


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

        # Validate file path restrictions
        if not os.path.exists(file_path):
            print(f"Error: File '{file_path}' not found", file=sys.stderr)
            return 1

        # Check if file is within current working directory
        abs_file_path = os.path.abspath(file_path)
        abs_cwd = os.path.abspath(".")

        if not abs_file_path.startswith(abs_cwd):
            print(
                "Error: File must be within current working directory (experimental restriction)",
                file=sys.stderr,
            )
            return 1

        # Check if it's a text file (not binary)
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                f.read(1024)  # Read first 1KB to check if it's text
        except UnicodeDecodeError:
            print(
                "Error: Binary files are not supported (experimental restriction)",
                file=sys.stderr,
            )
            return 1

        # Determine the interpreter based on file extension
        file_extension = os.path.splitext(file_path)[1].lower()
        if file_extension == ".py":
            interpreter = "python"
        elif file_extension in [".sh", ".bash"]:
            interpreter = "bash"
        else:
            # Try to detect shebang
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    first_line = f.readline().strip()
                    if first_line.startswith("#!"):
                        if "python" in first_line:
                            interpreter = "python"
                        elif "bash" in first_line or "sh" in first_line:
                            interpreter = "bash"
                        else:
                            print(
                                "Error: Unsupported interpreter in shebang. Only Python and Bash are supported.",
                                file=sys.stderr,
                            )
                            return 1
                    else:
                        print(
                            "Error: Cannot determine interpreter. Use .py or .sh/.bash extension, or add shebang.",
                            file=sys.stderr,
                        )
                        return 1
            except Exception as e:
                print(f"Error reading file: {e}", file=sys.stderr)
                return 1

        # Prepare working dir and runtime env options
        working_dir_opt = "--working-dir=."
        runtime_env_candidates = ["runtime_env.yaml", "runtime_env.yml"]
        runtime_env_file = next(
            (f for f in runtime_env_candidates if os.path.isfile(f)), None
        )
        runtime_env_present = runtime_env_file is not None

        # Parse GPUs from environment variable (supports 'n_gpus' or 'N_GPUS')
        gpu_env = os.environ.get("n_gpus") or os.environ.get("N_GPUS")
        entrypoint_num_gpus_arg = None
        gpu_str = None
        if gpu_env is not None and gpu_env != "":
            try:
                gpu_count = float(gpu_env)
                if gpu_count < 0:
                    raise ValueError
                # Use integer formatting if whole number, else keep float
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

        # Build Ray job submit command
        cmd = [
            "ray",
            "job",
            "submit",
            "--entrypoint-num-cpus=1",
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

        cmd.extend([interpreter, file_path])

        # Print concise context
        print(f"🚀 RaySSH: Submitting {interpreter} job: {file_path}")
        print(f"📦 Working dir: .")
        if runtime_env_present:
            print(f"🧩 Runtime env: ./{runtime_env_file}")
        else:
            print(
                f"🧩 Runtime env: remote (create runtime_env.yaml or runtime_env.yml to customize)"
            )
        if gpu_str is not None:
            print(f"🎛️ GPUs: {gpu_str}")
        print(f"📋 Command: {' '.join(cmd)}")
        print("⚠️  Experimental feature - file execution via Ray job submission")
        print()

        # Execute the ray job submit command
        result = subprocess.run(cmd, cwd=".")
        return result.returncode

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
        cmd = [
            "ray",
            "job",
            "submit",
            "--entrypoint-num-cpus=1",
            working_dir_opt,
        ]
        if entrypoint_num_gpus_arg:
            cmd.append(entrypoint_num_gpus_arg)

        # Prefer runtime_env.yaml if present
        runtime_env_candidates = ["runtime_env.yaml", "runtime_env.yml"]
        runtime_env_file = next(
            (f for f in runtime_env_candidates if os.path.isfile(f)), None
        )
        if runtime_env_file:
            cmd.append(f"--runtime-env={runtime_env_file}")

        cmd += ["--", "bash", "-lc", command]

        print(f"🚀 RaySSH: Submitting command job")
        print(f"   💬 {command}")
        if gpu_str is not None:
            print(f"   🎛️ GPUs: {gpu_str}")
        print(f"   📋 {' '.join(cmd)}")
        print()

        result = subprocess.run(cmd, cwd=".")
        return result.returncode
    except Exception as e:
        print(f"Error submitting command job: {e}", file=sys.stderr)
        return 1
