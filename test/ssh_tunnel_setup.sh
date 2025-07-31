#!/bin/bash
# SSH tunnel setup for Ray cluster (simpler than socat)

echo "=== SSH Tunnel Setup for Ray Cluster ==="
echo ""

REMOTE_HOST="11.141.18.18"
REMOTE_USER="your_username"  # Replace with your SSH username

echo "This approach uses SSH tunneling instead of socat (more reliable)"
echo ""

echo "1. First, on remote host ($REMOTE_HOST), restart Ray with standard ports:"
echo "   ssh $REMOTE_USER@$REMOTE_HOST"
echo "   ray stop"
echo "   ray start --head --dashboard-host=0.0.0.0"
echo ""

echo "2. Then create SSH tunnel from local machine:"
echo "   ssh -N -L 6379:localhost:6379 \\"
echo "       -L 8077:localhost:8077 \\"
echo "       -L 8076:localhost:8076 \\"
echo "       -L 8265:localhost:8265 \\"
echo "       $REMOTE_USER@$REMOTE_HOST &"
echo ""

echo "3. Set Ray address to use local forwarded ports:"
echo "   export RAY_ADDRESS=127.0.0.1:6379"
echo ""

echo "4. Test connection:"
echo "   rayssh --ls"
echo ""

echo "Advantages of SSH tunneling:"
echo "  - More secure (encrypted)"
echo "  - Handles multiple ports easily"
echo "  - Built-in reconnection"
echo "  - No need for socat" 