#!/usr/bin/env python3
"""
Network connectivity test for Ray cluster
"""

import socket
import sys
import os
from typing import Tuple

def test_port_connectivity(host: str, port: int, timeout: int = 5) -> Tuple[bool, str]:
    """Test if a port is accessible on a given host."""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        result = sock.connect_ex((host, port))
        sock.close()
        
        if result == 0:
            return True, f"✅ Port {port} on {host} is accessible"
        else:
            return False, f"❌ Port {port} on {host} is not accessible (connection refused)"
    except socket.gaierror as e:
        return False, f"❌ DNS resolution failed for {host}: {e}"
    except Exception as e:
        return False, f"❌ Connection test failed: {e}"

def test_ray_connectivity():
    """Test connectivity to Ray cluster endpoints."""
    print("=== Ray Cluster Connectivity Test ===\n")
    
    ray_address = os.environ.get('RAY_ADDRESS')
    if not ray_address:
        print("❓ RAY_ADDRESS not set - would connect to local cluster")
        return
    
    print(f"Testing connectivity to: {ray_address}")
    
    # Parse the address
    if ray_address.startswith('ray://'):
        # Ray Client mode
        address_part = ray_address.replace('ray://', '')
        if ':' in address_part:
            host, port_str = address_part.rsplit(':', 1)
            try:
                port = int(port_str)
            except ValueError:
                print(f"❌ Invalid port in RAY_ADDRESS: {port_str}")
                return
        else:
            host = address_part
            port = 10001  # Default Ray Client port
        
        print(f"\n🔍 Ray Client mode detected")
        print(f"   Host: {host}")
        print(f"   Port: {port}")
        
        # Test Ray Client port
        success, message = test_port_connectivity(host, port)
        print(f"\nRay Client port test: {message}")
        
        if not success:
            print(f"\n💡 Suggested fixes:")
            print(f"   1. Try standard Ray Client port:")
            print(f"      export RAY_ADDRESS='ray://{host}:10001'")
            print(f"   2. Try direct GCS connection:")
            print(f"      export RAY_ADDRESS='{host}:6379'")
            print(f"   3. Check firewall/network:")
            print(f"      telnet {host} {port}")
            print(f"      nc -zv {host} {port}")
        
        # Also test common Ray ports
        print(f"\n🔍 Testing other common Ray ports on {host}:")
        common_ports = [6379, 8265, 10001]  # GCS, Dashboard, Ray Client
        for test_port in common_ports:
            if test_port != port:  # Skip the port we already tested
                success, message = test_port_connectivity(host, test_port)
                print(f"   Port {test_port}: {'✅' if success else '❌'}")
        
    else:
        # Direct connection mode (usually GCS port)
        if ':' in ray_address:
            host, port_str = ray_address.rsplit(':', 1)
            try:
                port = int(port_str)
            except ValueError:
                print(f"❌ Invalid port in RAY_ADDRESS: {port_str}")
                return
        else:
            host = ray_address
            port = 6379  # Default GCS port
        
        print(f"\n🔍 Direct connection mode detected")
        print(f"   Host: {host}")
        print(f"   Port: {port}")
        
        # Test the specified port
        success, message = test_port_connectivity(host, port)
        print(f"\nConnection test: {message}")
        
        if not success:
            print(f"\n💡 Suggested fixes:")
            print(f"   1. Check if Ray cluster is running")
            print(f"   2. Verify firewall allows port {port}")
            print(f"   3. Try Ray Client mode:")
            print(f"      export RAY_ADDRESS='ray://{host}:10001'")

if __name__ == "__main__":
    test_ray_connectivity() 