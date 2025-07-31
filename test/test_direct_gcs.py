#!/usr/bin/env python3
"""
Test direct Ray GCS connection through socat forwarding
"""

import os
import ray
import time

def test_direct_gcs_connection():
    """Test direct Ray GCS connection to forwarded port."""
    print("=== Testing Direct Ray GCS Connection ===\n")
    
    ray_address = os.environ.get('RAY_ADDRESS', '11.141.18.18:443')
    print(f"Testing Ray GCS connection to: {ray_address}")
    
    try:
        # Try connecting directly to GCS with minimal configuration
        print("Attempting Ray Core API connection...")
        
        # Configure for direct GCS connection
        ray.init(
            address=ray_address,
            logging_level='ERROR',
            log_to_driver=False,
            ignore_reinit_error=True,
            _node_ip_address='127.0.0.1',  # Force local IP
            _driver_object_store_memory=100 * 1024 * 1024,  # 100MB
        )
        
        print("✅ Successfully connected to Ray GCS!")
        
        # Test basic cluster operations
        nodes = ray.nodes()
        print(f"Found {len(nodes)} nodes:")
        for i, node in enumerate(nodes, 1):
            node_id = node.get('NodeID', 'N/A')
            node_ip = node.get('NodeManagerAddress', 'N/A')
            alive = "✅" if node.get('Alive', False) else "❌"
            print(f"  Node {i}: {node_ip} (ID: {node_id[:12]}...) {alive}")
        
        # Test actor creation
        print("\nTesting remote actor creation...")
        
        @ray.remote
        class TestActor:
            def get_node_info(self):
                import os
                return {
                    'hostname': os.uname().nodename,
                    'pid': os.getpid()
                }
        
        actor = TestActor.remote()
        result = ray.get(actor.get_node_info.remote())
        print(f"✅ Actor created on: {result['hostname']} (PID: {result['pid']})")
        
    except Exception as e:
        print(f"❌ Ray GCS connection failed: {e}")
        print(f"Error type: {type(e).__name__}")
        
        # Detailed diagnosis
        print("\n🔧 Diagnosis:")
        if "GcsClient" in str(e):
            print("- GCS client connection issue")
            print("- Check if Ray head node GCS is properly exposed")
        elif "timeout" in str(e).lower():
            print("- Connection timeout - socat might not be forwarding properly")
            print("- Check socat logs and Ray head node status")
        elif "refused" in str(e).lower():
            print("- Connection refused - Ray head node might not be running")
            print("- Verify: ray status on remote head node")
        
        print("\n💡 Debugging steps:")
        print("1. On remote head node, check:")
        print("   ray status")
        print("   ps aux | grep ray")
        print("   netstat -ln | grep 3733")
        print("2. Check socat process:")
        print("   ps aux | grep socat")
        print("   tail socat.out")
        print("3. Try direct connection to GCS port:")
        print("   telnet 11.141.18.18 443")
        
    finally:
        if ray.is_initialized():
            try:
                ray.shutdown()
            except:
                pass

if __name__ == "__main__":
    test_direct_gcs_connection() 