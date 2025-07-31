# Fix Your Current Socat Setup

## Current Status
You have:
```bash
ray start --port 3733
nohup socat TCP-LISTEN:443,fork,reuseaddr TCP:localhost:3733 > socat.out &
```

## Problem
Ray needs multiple ports, but you're only forwarding GCS port.

## Quick Fix Steps

### 1. On Remote Head Node
Find the actual ports Ray is using:
```bash
ray status
# or
netstat -tulpn | grep ray
```

You'll see output like:
```
GCS server port: 3733
Raylet socket name: /tmp/ray/session_xyz/sockets/raylet
Object Manager port: 8076  # Random port
Node Manager port: 8077    # Random port
```

### 2. Add Missing Port Forwards (Local Machine)

Keep your existing socat, but add these:

```bash
# Your existing forward (keep this)
nohup socat TCP-LISTEN:443,fork,reuseaddr TCP:11.141.18.18:3733 > socat-gcs.out &

# Add these new forwards (replace XXXX with actual ports from ray status)
nohup socat TCP-LISTEN:8076,fork,reuseaddr TCP:11.141.18.18:8076 > socat-object.out &
nohup socat TCP-LISTEN:8077,fork,reuseaddr TCP:11.141.18.18:8077 > socat-raylet.out &
```

### 3. Update Ray Address
```bash
export RAY_ADDRESS=127.0.0.1:443  # Point to your local forwarded port
```

### 4. Test
```bash
rayssh --ls
```

## Better Alternative: Restart Ray with Fixed Ports

Instead of guessing ports, restart Ray with known ports:

```bash
# On remote head node:
ray stop
ray start --head --port=3733 --object-manager-port=8076 --node-manager-port=8077

# On local machine:
nohup socat TCP-LISTEN:443,fork,reuseaddr TCP:11.141.18.18:3733 > socat-gcs.out &
nohup socat TCP-LISTEN:8076,fork,reuseaddr TCP:11.141.18.18:8076 > socat-object.out &
nohup socat TCP-LISTEN:8077,fork,reuseaddr TCP:11.141.18.18:8077 > socat-raylet.out &

export RAY_ADDRESS=127.0.0.1:443
``` 