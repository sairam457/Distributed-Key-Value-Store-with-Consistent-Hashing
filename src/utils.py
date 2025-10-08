import json
import time

def print_ring_status(client):
    """Print formatted ring status"""
    status = client.get_ring_status()
    
    print("\n" + "="*50)
    print("📊 CONSISTENT HASHING RING STATUS")
    print("="*50)
    
    ring_info = status.get('ring_status', {})
    nodes_info = status.get('nodes', {})
    
    print(f"🔄 Replication Factor: {status.get('replication_factor', 2)}")
    print(f"🏠 Total Physical Nodes: {ring_info.get('total_nodes', 0)}")
    print(f"🔗 Total Virtual Nodes: {ring_info.get('total_virtual_nodes', 0)}")
    
    print("\n🏷️  Active Nodes:")
    for node_id, node_info in nodes_info.items():
        status_emoji = "✅" if node_info.get('status') == 'active' else "❌"
        print(f"  {status_emoji} {node_id} - {node_info.get('status', 'unknown')}")
    
    print("\n🎯 Virtual Node Distribution:")
    ring_mapping = ring_info.get('ring_mapping', {})
    sorted_hashes = ring_info.get('sorted_hashes', [])
    
    for hash_val in sorted_hashes[:10]:  # Show first 10 for brevity
        node = ring_mapping.get(hash_val, 'unknown')
        print(f"  {hash_val:10} → {node}")
    
    if len(sorted_hashes) > 10:
        print(f"  ... and {len(sorted_hashes) - 10} more virtual nodes")
    
    print("="*50)

def simulate_fault_tolerance(client, test_keys):
    """Demonstrate fault tolerance with node changes"""
    print("\n" + "⚡"*60)
    print("⚡                FAULT TOLERANCE SIMULATION")
    print("⚡"*60)
    
    # Step 1: Store initial data
    print("\n1. 📝 STORING INITIAL DATA...")
    for key in test_keys:
        result = client.put(key, f"value_{key}")
        status_emoji = "✅" if result['status'] == 'success' else "❌"
        print(f"   {status_emoji} PUT {key}: {result.get('message', 'Unknown')}")
        time.sleep(0.1)
    
    # Step 2: Verify data is accessible
    print("\n2. 🔍 VERIFYING DATA ACCESSIBILITY...")
    for key in test_keys:
        result = client.get(key)
        status_emoji = "✅" if result['status'] == 'success' else "❌"
        print(f"   {status_emoji} GET {key}: {result.get('message', 'Unknown')}")
        time.sleep(0.1)
    
    # Step 3: Add new node
    print("\n3. 🆕 ADDING NEW NODE 'node4@localhost:8003'...")
    client.add_node("localhost:8003", "localhost", 8003)
    print_ring_status(client)
    
    # Step 4: Store more data with new ring
    print("\n4. 📝 STORING DATA WITH UPDATED RING...")
    new_keys = [f"new_{key}" for key in test_keys]
    for key in new_keys:
        result = client.put(key, f"value_{key}")
        status_emoji = "✅" if result['status'] == 'success' else "❌"
        print(f"   {status_emoji} PUT {key}: {result.get('message', 'Unknown')}")
        time.sleep(0.1)
    
    # Step 5: Remove a node
    print("\n5. 🗑️  REMOVING NODE 'localhost:8001'...")
    client.remove_node("localhost:8001")
    print_ring_status(client)
    
    # Step 6: Verify all data is still accessible
    print("\n6. 🔍 VERIFYING DATA ACCESSIBILITY AFTER NODE REMOVAL...")
    all_keys = test_keys + new_keys
    accessible_count = 0
    
    for key in all_keys:
        result = client.get(key)
        if result['status'] == 'success':
            accessible_count += 1
            status_emoji = "✅"
        else:
            status_emoji = "❌"
        print(f"   {status_emoji} GET {key}: {result.get('message', 'Unknown')}")
        time.sleep(0.1)
    
    print(f"\n📈 ACCESSIBILITY SUMMARY: {accessible_count}/{len(all_keys)} keys accessible")
    print("⚡"*60)
    print("🎉 FAULT TOLERANCE SIMULATION COMPLETED!")
    print("⚡"*60)