# RaySSH: Command a Ray node like a shell

RaySSH provides an SSH-like interface to Ray cluster nodes, allowing you to execute commands remotely on specific nodes through an interactive shell.

## Installation

Install RaySSH in development mode:

```bash
pip install -e .
```

Or install from source:

```bash
git clone <repository-url>
cd rayssh
pip install -e .
```

## Usage

After installation, you can use the `rayssh` command directly:

```bash
rayssh <node_ip_address | node_id> [--help]
```

### Examples

Connect to a node by IP address:
```bash
rayssh 192.168.1.100
```

Connect to a node by Ray node ID:
```bash
rayssh a1b2c3d4e5f6
```

Show help:
```bash
rayssh --help
```

### Interactive Shell Commands

Once connected, you can use the remote shell just like a regular shell:
- Most standard shell commands work (ls, cat, grep, etc.)
- Built-in commands: cd, pwd, export, **pushd**, **popd**, **dirs**
- **Tab completion** for file names (press Tab to autocomplete)
- Ctrl-C interrupts the current command
- Ctrl-D or 'exit' or 'quit' to disconnect
- Working directory and environment variables are maintained across commands

## Requirements

- Python >= 3.8
- Ray >= 2.0.0

## Limitations

- This is not a real shell. It mimics the shell interface, but it's implemented with Ray Core API.
- Complex features like pipes, redirections, job control, and interactive programs may not work as expected.

## Implementation

1. Find the Ray node by its IP address or node ID (format: `xxx.yyy.zzz.aaa` or hexadecimal node_id)
2. Deploy a Ray actor that lives on the target node to handle shell operations
3. The shell actor handles command execution and returns results
4. Provides an interactive shell interface with signal handling (Ctrl-C, Ctrl-D)

## License

MIT License - see [LICENSE](LICENSE) file for details.