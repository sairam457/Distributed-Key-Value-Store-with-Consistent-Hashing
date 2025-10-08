#!/usr/bin/env python3
"""
Distributed Key-Value Store Demo - Simple Version
"""

import sys
import os
import time
import threading

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(__file__))

try:
    from src.ring_manager import RingManager
    from src.server_node import KVServer
    print("✅ All modules imported successfully!")
except ImportError as e:
    print(f"❌ Import error: {e}")
    print("Please make sure all files are in the correct location:")
    print("  src/consistent_hashing.py")
    print("  src/ring_manager.py")
    print("  src/server_node.py")
    print("  src/client.py")
    print("  src/utils.py")
    print("  src/__init__.py (empty file)")
    sys.exit(1)

def start_servers():
    """Start multiple server nodes with shared ring manager"""
    print("🚀 Starting Distributed Key-Value Store Servers...")
    
    # Create shared ring manager
    ring_manager = RingManager(replication_factor=2, virtual_nodes=3)
    
    servers = []
    server_configs = [
        ('localhost', 8000),
        ('localhost', 8001), 
        ('localhost', 8002)
    ]
    
    # Start servers in separate threads
    for host, port in server_configs:
        server = KVServer(host, port, ring_manager, replication_factor=2)
        server_thread = threading.Thread(
            target=server.start,
            name=f"Server-{port}",
            daemon=True
        )
        server_thread.start()
        servers.append(server)
        print(f"✅ Server {host}:{port} started")
        time.sleep(1)  # Give each server time to start
    
    print("\n🎉 All servers started successfully!")
    print("📋 Running Servers:")
    for host, port in server_configs:
        print(f"   • {host}:{port}")
    
    print(f"\n⚙️  Configuration:")
    print(f"   • Replication Factor: 2")
    print(f"   • Virtual Nodes per Server: 3")
    
    print("\n💡 Next steps:")
    print("   1. Open a NEW terminal")
    print("   2. Run: python -m src.client --nodes localhost:8000 localhost:8001 localhost:8002")
    print("   3. Try these commands in the client:")
    print("      - 'status' to see ring information")
    print("      - 'put user1 John' to store data")
    print("      - 'get user1' to retrieve data")
    print("      - 'add_node localhost:8003 localhost:8003' to add a node")
    print("      - 'remove_node localhost:8001' to remove a node")
    
    print("\n⏹️  Press Ctrl+C to stop all servers...")
    
    try:
        # Keep main thread alive
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n🛑 Stopping all servers...")
        for server in servers:
            server.stop()
        print("👋 All servers stopped. Goodbye!")

if __name__ == "__main__":
    print("=" * 60)
    print("   DISTRIBUTED KEY-VALUE STORE DEMO")
    print("=" * 60)
    start_servers()