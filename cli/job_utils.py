import os
import sys
from typing import Optional, Tuple


def validate_and_detect_interpreter(
    file_path: str,
) -> Tuple[Optional[str], Optional[str]]:
    """
    Validate the target file and detect interpreter.

    Returns (interpreter, error_message). interpreter is one of 'python'|'bash' or None.
    """
    if not os.path.exists(file_path):
        return None, f"Error: File '{file_path}' not found"

    abs_file_path = os.path.abspath(file_path)
    abs_cwd = os.path.abspath(".")
    if not abs_file_path.startswith(abs_cwd):
        return None, (
            "Error: File must be within current working directory (experimental restriction)"
        )

    # Check text file
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            f.read(1024)
    except UnicodeDecodeError:
        return None, (
            "Error: Binary files are not supported (experimental restriction)"
        )
    except Exception as e:
        return None, f"Error reading file: {e}"

    # Determine interpreter
    ext = os.path.splitext(file_path)[1].lower()
    if ext == ".py":
        return "python", None
    if ext in [".sh", ".bash"]:
        return "bash", None

    # Try shebang
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            first_line = f.readline().strip()
            if first_line.startswith("#!"):
                if "python" in first_line:
                    return "python", None
                if "bash" in first_line or "sh" in first_line:
                    return "bash", None
                return None, (
                    "Error: Unsupported interpreter in shebang. Only Python and Bash are supported."
                )
            else:
                return None, (
                    "Error: Cannot determine interpreter. Use .py or .sh/.bash extension, or add shebang."
                )
    except Exception as e:
        return None, f"Error reading file: {e}"


def get_runtime_env_options() -> tuple[str, Optional[str], bool]:
    """
    Determine working dir and optional runtime_env file.
    Returns (working_dir_opt, runtime_env_file, runtime_env_present)
    """
    working_dir_opt = "--working-dir=."
    runtime_env_candidates = ["runtime_env.yaml", "runtime_env.yml"]
    runtime_env_file = next(
        (f for f in runtime_env_candidates if os.path.isfile(f)), None
    )
    return working_dir_opt, runtime_env_file, runtime_env_file is not None


def parse_n_nodes_from_env() -> Optional[int]:
    """Parse N_NODES/n_nodes from environment; return int or None. Raise on invalid."""
    raw = os.environ.get("n_nodes") or os.environ.get("N_NODES")
    if raw is None or raw == "":
        return None
    try:
        val = int(float(raw))
        if val <= 0:
            raise ValueError
        return val
    except Exception:
        raise ValueError(f"Invalid n_nodes value '{raw}'. Must be an integer >= 1.")


def parse_job_entrypoint_gpus() -> tuple[Optional[str], Optional[str]]:
    """
    Parse n_gpus for single-process job entrypoint.
    Returns (entrypoint_num_gpus_arg, gpu_str_shown)
    """
    gpu_env = os.environ.get("n_gpus") or os.environ.get("N_GPUS")
    if gpu_env is None or gpu_env == "":
        return None, None
    try:
        count = float(gpu_env)
        if count < 0:
            raise ValueError
        shown = str(int(count)) if count.is_integer() else str(count)
        return f"--entrypoint-num-gpus={shown}", shown
    except Exception:
        raise ValueError(f"Invalid n_gpus value '{gpu_env}'. Must be a number >= 0.")


def parse_per_node_gpus_for_multinode() -> tuple[Optional[str], Optional[str]]:
    """
    Parse n_gpus_per_node (or fallback to n_gpus with a warning message).
    Returns (per_node_gpu_str, warning_msg)
    """
    per_node = os.environ.get("n_gpus_per_node") or os.environ.get("N_GPUS_PER_NODE")
    if per_node is not None and per_node != "":
        try:
            g = float(per_node)
            if g < 0:
                raise ValueError
            return (str(int(g)) if g.is_integer() else str(g)), None
        except Exception:
            raise ValueError(
                f"Invalid n_gpus_per_node value '{per_node}'. Must be a number >= 0."
            )

    gpu_env = os.environ.get("n_gpus") or os.environ.get("N_GPUS")
    if gpu_env is not None and gpu_env != "":
        try:
            g = float(gpu_env)
            if g < 0:
                raise ValueError
            shown = str(int(g)) if g.is_integer() else str(g)
            return shown, (
                "n_gpus_per_node not set but n_gpus found; using n_gpus as per-node GPUs."
            )
        except Exception:
            raise ValueError(
                f"Invalid n_gpus value '{gpu_env}'. Must be a number >= 0."
            )
    return None, None


def get_master_port_default() -> str:
    return os.environ.get("MASTER_PORT") or os.environ.get("master_port") or "29500"
