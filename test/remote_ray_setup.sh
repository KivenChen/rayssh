#!/bin/bash
# Remote Ray cluster setup with fixed ports for port forwarding

echo "=== Setting up Ray cluster with fixed ports ==="

# Stop any existing Ray processes
ray stop

# Start Ray head node with fixed ports
ray start --head \
    --port=6379 \
    --object-manager-port=8076 \
    --node-manager-port=8077 \
    --gcs-server-port=6379 \
    --raylet-port=8077 \
    --min-worker-port=10000 \
    --max-worker-port=10999 \
    --dashboard-port=8265 \
    --include-dashboard=true

echo "Ray cluster started with fixed ports:"
echo "  GCS Server: 6379"
echo "  Raylet: 8077"
echo "  Object Manager: 8076"
echo "  Dashboard: 8265"
echo "  Worker ports: 10000-10999"

# Check status
ray status

echo "=== Port forwarding commands for local machine ==="
echo "Run these on your LOCAL machine:"
echo ""
echo "# Forward GCS port (main cluster connection)"
echo "nohup socat TCP-LISTEN:6379,fork,reuseaddr TCP:REMOTE_IP:6379 > socat-gcs.out &"
echo ""
echo "# Forward Raylet port (node manager)"
echo "nohup socat TCP-LISTEN:8077,fork,reuseaddr TCP:REMOTE_IP:8077 > socat-raylet.out &"
echo ""
echo "# Forward Object Manager port"
echo "nohup socat TCP-LISTEN:8076,fork,reuseaddr TCP:REMOTE_IP:8076 > socat-object.out &"
echo ""
echo "# Forward Dashboard (optional)"
echo "nohup socat TCP-LISTEN:8265,fork,reuseaddr TCP:REMOTE_IP:8265 > socat-dashboard.out &"
echo ""
echo "Replace REMOTE_IP with your actual remote IP: 11.141.18.18"
echo ""
echo "Then set: export RAY_ADDRESS=127.0.0.1:6379" 