# RaySSH: Command your Ray nodes like a shell
## Features

🖥️ **Interactive Shell**: Command Ray nodes with a familiar shell interface  
🎲 **Smart Connection**: Automatically connects to worker nodes or specific targets  
🌐 **Remote Mode**: Work on remote with your local working directory, like on your local machine
🚀 **Job Submission**: Run Python/Bash files as Ray jobs, like on your local machine

## Installation

First, figure out your Ray cluster's Ray version and Python version - we need to align them.

Then, install RaySSH using `uv`, so that it runs in a dedicated environment that aligns with your Ray cluster.

```bash
# Install RaySSH as a tool with the Ray executables and a specific Python
uv tool install \
  --with-executables-from "ray[default]==<target_cluster_ray_version>" \
  --python <target_cluster_python_version> \
  git+https://github.com/kivenchen/RaySSH.git
```

After installation, you can use the `rayssh` command from your terminal directly.

## Quick Start

**Basic Commands:**
- `rayssh` — Random worker connection
- `rayssh -l` — Interactive node selection
- `rayssh <ip|node_id|-index>` — Connect to a specific node
- `rayssh <dir>` — Remote mode with directory upload
- `rayssh <file>` — Submit and run file as a Ray job
- `rayssh lab [-q] [path]` — Launch JupyterLab; optional upload path
- `rayssh code [-q] [path]` — Launch code-server (VS Code); optional upload path
- `rayssh --ls` — Print nodes table

**Configuration:**
- `export RAY_ADDRESS=ray://remote-cluster-host:port`

## Examples

```bash
# 1) Configure once for your cluster
export RAY_ADDRESS=ray://remote-cluster-host:10001

# 2) Connect and inspect
rayssh              # random worker
> ray status

# 3) Remote dev with upload, then open VS Code in the browser
rayssh code -q ~/my-project   # uploads path, prints URL, exits (server keeps running)

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
rayssh sync .
# Your project files are now uploaded to remotely
> ls                    # See your uploaded files
> mount -t nfs 192.168.1.100:/workspace/datasets /mnt/datasets
> vim train_config.py          # Edit remote copies of your files, which syncs back to local automatically
> python train.py       # Run training on cluster
```

# 6) Pick a specific node, then start JupyterLab
rayssh -l            # choose node interactively
rayssh lab -q        # prints URL, exits (server keeps running)
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