#!/usr/bin/env python3
"""
Test script to demonstrate rayssh remote cluster connectivity
"""

import os
import ray

def test_ray_address_behavior():
    """Test how Ray behaves with RAY_ADDRESS environment variable."""
    
    print("=== Testing Ray Remote Cluster Connectivity ===\n")
    
    # Test 1: Check current RAY_ADDRESS setting
    ray_address = os.environ.get('RAY_ADDRESS')
    print(f"Current RAY_ADDRESS: {ray_address or 'Not set (will use local cluster)'}")
    
    # Test 2: Try to initialize Ray
    try:
        if ray.is_initialized():
            print("Ray is already initialized")
            ray.shutdown()
        
        print("Attempting to initialize Ray...")
        ray.init(ignore_reinit_error=True)
        
        # Test 3: Get cluster information
        print(f"✅ Successfully connected to Ray cluster!")
        print(f"Ray cluster address: {ray.get_runtime_context().gcs_address}")
        
        # Test 4: List available nodes
        nodes = ray.nodes()
        print(f"Found {len(nodes)} nodes in cluster:")
        
        for i, node in enumerate(nodes, 1):
            node_id = node.get('NodeID', 'N/A')
            node_ip = node.get('NodeManagerAddress', 'N/A')
            alive = "✅" if node.get('Alive', False) else "❌"
            print(f"  Node {i}: {node_ip} (ID: {node_id[:12]}...) {alive}")
        
        print(f"\n🎉 rayssh should work with this cluster!")
        print(f"Try: rayssh --show")
        print(f"Try: rayssh {nodes[0].get('NodeManagerAddress', 'NODE_IP')}")
        
    except Exception as e:
        print(f"❌ Failed to connect to Ray cluster: {e}")
        print("Make sure RAY_ADDRESS points to a valid cluster or start a local cluster")
    
    finally:
        if ray.is_initialized():
            ray.shutdown()

if __name__ == "__main__":
    test_ray_address_behavior()