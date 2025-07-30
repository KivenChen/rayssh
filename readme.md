# RaySSH: Command a Ray node like a shell

RaySSH provides an SSH-like interface to Ray cluster nodes, allowing you to execute commands remotely on specific nodes through an interactive shell.

## Installation


```bash
git clone https://github.com/kivenchen/RaySSH.git
cd rayssh
pip install -e .
```

## Usage

After installation, you can use the `rayssh` command directly:

```bash
kiven@Kivs-Macbook rayssh % rayssh --help
RaySSH: Command a Ray node like a shell

Usage:
    rayssh <node_ip_address | node_id> [options]
    rayssh --list | --ls | --show

Arguments:
    node_ip_address    IP address of the target Ray node (format: xxx.yyy.zzz.aaa)
    node_id           Ray node ID (hexadecimal string)

Options:
    --help, -h        Show this help message and exit
    --list, --ls, --show
                      List all available Ray nodes in a table format

Examples:
    rayssh 192.168.1.100           # Connect to node by IP
    rayssh a1b2c3d4e5f6            # Connect to node by ID
    rayssh --list                  # List all available nodes

Once connected, you can use the remote shell just like a regular shell:
- Most standard shell commands work (ls, cat, grep, etc.)
- Built-in commands: cd, pwd, export, pushd, popd, dirs
- Tab completion for file names (press Tab to autocomplete)
- Ctrl-C interrupts the current command
- Ctrl-D or 'exit' or 'quit' to disconnect
- Working directory and environment variables are maintained across commands

Note: This is not a full shell implementation. Complex features like pipes,
redirections, job control, and interactive programs may not work as expected.
```

### Interactive Shell Commands

Once connected, you can use the remote shell just like a regular shell:
- Most standard shell commands work (ls, cat, grep, etc.)
- Built-in commands: cd, pwd, export, pushd, popd, dirs
- Tab completion for file names (press Tab to autocomplete)
- Basic signal handling (Ctrl-C, Ctrl-D)
- Working directory and environment variables are maintained across commands

## Requirements

- Python >= 3.8
- Ray >= 2.0.0

## Limitations

- This is not a real shell. It mimics the shell interface, but it's implemented with Ray Core API.
- The shell's environment sticks to its host node's Ray runtime environment.
- Complex features like pipes, redirections, job control, and interactive programs may not work as expected.

## License

MIT License - see [LICENSE](LICENSE) file for details.