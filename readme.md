# RaySSH: Command your Ray nodes like a shell
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

**Basic Commands:**
- `rayssh` - Random worker connection
- `rayssh <ip|node_id|-index>` - Connect to specific node
- `rayssh <dir>` - Remote mode with directory upload
- `rayssh <file>` - Submit and run file as Ray job
- `rayssh lab [-q] [path]` - Launch Jupyter Lab on a worker node; tails log for URL. With `-q`, exit after showing link. The optional `path` will be uploaded as working dir.
- `rayssh code [-q] [path]` - Launch code-server (VS Code) on a worker node; behaves like `lab`. The optional `path` will be uploaded as working dir.
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
### 4. 🧰 VS Code (code-server) on the Cluster

```bash
# Local cluster
rayssh code           # Blocks and tails log; Ctrl-C stops server
rayssh code -q        # Show link and exit; server keeps running

# Remote mode with workspace upload
export RAY_ADDRESS=ray://gpu-cluster.remote:10001
rayssh code ~/my-project
```

Notes:
- code-server binds like Jupyter; on macOS, 80 becomes 8888 automatically.
- A password is generated for each session.

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

Once connected, you get a full shell experience - do whatever you'd like

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
- Network access to your Ray cluster
  - For "rayssh <file>" job submission, access to head node is sufficient.
  - For other features, access to worker nodes is also required.


## Troubleshooting

**Performance:**

- Upload only necessary files for better performance. Use ".gitignore" to exclude files from working dir.

## License

MIT License - see [LICENSE](LICENSE) file for details.