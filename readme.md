# RaySSH: Command Ray nodes like a shell or submit jobs
## Features

🖥️ **Interactive Shell**: Command Ray nodes with a familiar shell interface  
🎲 **Smart Connection**: Automatically connects to worker nodes or specific targets  
🌐 **Remote Mode**: Work on remote with your local working directory, like on your local machine
🚀 **Job Submission**: Run Python/Bash files as Ray jobs, like on your local machine

## Installation

```bash
git clone https://github.com/kivenchen/RaySSH.git
cd rayssh
pip install -e .
```

After installation, you can use the `rayssh` command from your terminal directly.

## Quick Start

### Local Ray Cluster

```bash
# Start local Ray cluster
ray start --head

# Use RaySSH
rayssh                    # Random worker node
rayssh -0                 # Head node
rayssh -1                 # First worker
rayssh -l                 # Interactive selection
rayssh 192.168.1.100      # Specific IP
```

### Remote Ray Cluster

```bash
# Set remote cluster address
export RAY_ADDRESS=ray://gpu-cluster.remote:10001

# Work remotely like local
rayssh                    # Connect remotely
rayssh ~/my-project       # Upload folder as working dir and work remotely
rayssh .                  # Upload current directory and work remotely
```

```bash
rayssh --help
```

**Basic Commands:**
- `rayssh` - Random worker connection
- `rayssh <ip|node_id|-index>` - Connect to specific node
- `rayssh <dir>` - Remote mode with directory upload
- `rayssh <file>` - Submit file as Ray job
- `rayssh lab [-q] [path]` - Launch Jupyter Lab on a worker node; tails log for URL. With `-q`, exit after showing link.
  - If no worker is available, you can run: `rayssh -0 lab [-q] [path]` to place Lab on the head node.
- `rayssh -l` - Interactive node selection
- `rayssh --ls` - Print nodes table

**Configuration:**
- `export RAY_ADDRESS=ray://remote-cluster-host:port`

## Usage Scenarios

### 1. 🧪 Development & Debugging

**Debug on specific nodes:**
```bash
# Check which nodes are available
rayssh --ls

# Quick random worker connection
rayssh
> ray status
> python train_model.py
```

### 2. 🌐 Remote Cluster Development

**Set up remote development environment:**
```bash
# One-time setup
export RAY_ADDRESS=ray://gpu-cluster.company.com:10001
```

**Then work remotely like you're local:**
```bash
cd ~/machine-learning-project
# Manage what files to upload, and what to ignore
echo "*.parquet" >> .gitignore
# You can also customize your runtime environment in a runtime_env.yaml
vim runtime_env.yaml
# Upload your project and start working
rayssh .
# Your project files are now uploaded to remotely
> ls                    # See your uploaded files
> mount -t nfs 192.168.1.100:/workspace/datasets /mnt/datasets
> vim train_config.py          # Edit remote copies of your files
> python train.py       # Run training on cluster
```

**Or run local jobs on remote:**
```bash
# Submit job with your curdir as workspace
rayssh train_model.py
# Output: Job submitted, logs streaming, Ctrl-C to abort...

# Quick submit without waiting
rayssh -q preprocess_data.py
# Output: Job submitted, check Ray Dashboard for logs

ray job list
ray job stop <job_id>
```

**Mixed workflow:**
### 3. 🧪 Jupyter Lab on the Cluster

Start a Jupyter Lab on a worker node that other machines can access:

```bash
# Local cluster
rayssh lab                # Blocks and tails log until interrupted (server stays up)
rayssh lab -q             # Tail briefly to show URL, then exit

# Remote mode with workspace upload
export RAY_ADDRESS=ray://gpu-cluster.remote:10001
rayssh lab ~/my-notebooks # Uploads the path and opens Lab at that root
```

Notes:
- Lab binds to port 80 and detects a reachable host IP so URLs are accessible off-box.
- If a path is provided in remote mode, it is uploaded via Ray Client and used as `--ServerApp.root_dir`.
- `-q` tails the log until the access URL is visible, then exits; the server keeps running.

```bash
# Submit data preprocessing job
rayssh -q preprocess.py

# While that runs, work interactively
rayssh ~/train
> vim runtime_env.yaml
> vim train.py
> python train.py
```

## Interactive Shell Features

Once connected, you get a full shell experience:

**🖥️ Shell Commands:**
- Standard commands: `ls`, `cat`, `grep`, `find`, `ps`, etc.
- Built-in commands: `cd`, `pwd`, `export`, `pushd`, `popd`, `dirs`
- File editing: `vim`, `nano` (opens locally, edits remote files)

**⌨️ Interactive Features:**
- Tab completion for files and commands
- Command history (up/down arrows)
- Ctrl-C interrupts current command
- Ctrl-D or `exit`/`quit` to disconnect

**🔧 Advanced Features:**
- Interactive programs: `python`, `ipython`, `htop`, `top`
- Environment persistence across commands
- Working directory maintained between sessions
- Signal handling for graceful interruption

## Configuration

**Environment Variables:**
```bash
# Remote cluster connection
export RAY_ADDRESS=ray://cluster:10001

# Ray configuration (optional)
export RAY_CLIENT_RECONNECT_GRACE_PERIOD=60
```

## Requirements

- Python >= 3.8
- Ray >= 2.0.0
- Network access to Ray cluster (for remote mode)

## Limitations

- Not a full shell implementation - uses Ray Core API
- Interactive programs may have limited functionality
- Complex shell features (pipes, redirections) not fully supported
- File editing opens local editor for remote files

## Troubleshooting

**Performance:**
- Use `--ls` to check cluster resources before connecting
- Consider node workload when selecting target nodes
- Upload only necessary files for better performance

## License

MIT License - see [LICENSE](LICENSE) file for details.