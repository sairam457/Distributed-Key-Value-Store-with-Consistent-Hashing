import socket
import json
import argparse
import sys

class KVClient:
    def __init__(self, bootstrap_nodes=None, replication_factor=2):
        from .ring_manager import RingManager
        self.ring_manager = RingManager(replication_factor)
        self.replication_factor = replication_factor
        
        # Add bootstrap nodes to the ring
        if bootstrap_nodes:
            for node in bootstrap_nodes:
                try:
                    host, port = node.split(':')
                    self.ring_manager.register_node(node, host, int(port))
                    print(f"✅ Added bootstrap node: {node}")
                except ValueError:
                    print(f"❌ Invalid node format: {node}. Use 'host:port'")
    
    def add_node(self, node_id, host, port):
        """Add a node to the ring"""
        self.ring_manager.register_node(node_id, host, port)
        return f"✅ Node {node_id} added to ring"
    
    def remove_node(self, node_id):
        """Remove a node from the ring"""
        self.ring_manager.unregister_node(node_id)
        return f"✅ Node {node_id} removed from ring"
    
    def _send_request(self, node_address, request):
        """Send request to a specific node"""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(5)
                sock.connect(node_address)
                sock.send(json.dumps(request).encode('utf-8'))
                response = sock.recv(4096).decode('utf-8')
                return json.loads(response)
        except socket.timeout:
            return {'status': 'error', 'message': f'Connection timeout to {node_address}'}
        except ConnectionRefusedError:
            return {'status': 'error', 'message': f'Connection refused by {node_address}'}
        except Exception as e:
            return {'status': 'error', 'message': f'Connection failed: {str(e)}'}
    
    def put(self, key, value):
        """Store a key-value pair"""
        # Find responsible nodes
        responsible_nodes = self.ring_manager.get_responsible_nodes(key)
        
        if not responsible_nodes:
            return {'status': 'error', 'message': 'No active nodes available in ring'}
        
        primary_node = responsible_nodes[0]
        node_address = self.ring_manager.get_node_address(primary_node)
        
        if not node_address:
            return {'status': 'error', 'message': f'Primary node {primary_node} is unavailable'}
        
        print(f"🔑 Key '{key}' mapped to nodes: {responsible_nodes}")
        
        request = {
            'action': 'PUT',
            'key': key,
            'value': value
        }
        
        result = self._send_request(node_address, request)
        result['responsible_nodes'] = responsible_nodes
        return result
    
    def get(self, key):
        """Retrieve a key-value pair"""
        # Find responsible nodes
        responsible_nodes = self.ring_manager.get_responsible_nodes(key)
        
        if not responsible_nodes:
            return {'status': 'error', 'message': 'No active nodes available'}
        
        print(f"🔍 Key '{key}' mapped to nodes: {responsible_nodes}")
        
        # Try each responsible node until we find the data
        for node_id in responsible_nodes:
            node_address = self.ring_manager.get_node_address(node_id)
            if node_address:
                request = {'action': 'GET', 'key': key}
                result = self._send_request(node_address, request)
                if result.get('status') == 'success':
                    result['node'] = node_id
                    result['responsible_nodes'] = responsible_nodes
                    return result
                elif result.get('status') == 'error':
                    # Continue to next node if this one doesn't have the key
                    continue
        
        return {'status': 'error', 'message': f'Key {key} not found on any responsible node'}
    
    def delete(self, key):
        """Delete a key-value pair"""
        responsible_nodes = self.ring_manager.get_responsible_nodes(key)
        
        if not responsible_nodes:
            return {'status': 'error', 'message': 'No active nodes available'}
        
        primary_node = responsible_nodes[0]
        node_address = self.ring_manager.get_node_address(primary_node)
        
        if not node_address:
            return {'status': 'error', 'message': f'Primary node {primary_node} is unavailable'}
        
        request = {'action': 'DELETE', 'key': key}
        result = self._send_request(node_address, request)
        result['responsible_nodes'] = responsible_nodes
        return result
    
    def get_ring_status(self):
        """Get the current status of the ring"""
        return self.ring_manager.get_ring_info()
    
    def health_check(self, node_id):
        """Check health of a specific node"""
        node_address = self.ring_manager.get_node_address(node_id)
        if not node_address:
            return {'status': 'error', 'message': f'Node {node_id} not found'}
        
        request = {'action': 'HEALTH'}
        return self._send_request(node_address, request)

def main():
    parser = argparse.ArgumentParser(
        description='Distributed Key-Value Store Client',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  python -m src.client --nodes localhost:8000 localhost:8001 localhost:8002
  python -m src.client --nodes localhost:8000 localhost:8001 --replication 3
        '''
    )
    parser.add_argument('--nodes', nargs='+', required=True, help='Bootstrap nodes (host:port)')
    parser.add_argument('--replication', type=int, default=2, help='Replication factor (default: 2)')
    
    args = parser.parse_args()
    
    try:
        client = KVClient(args.nodes, args.replication)
    except Exception as e:
        print(f"❌ Failed to initialize client: {e}")
        sys.exit(1)
    
    print("=" * 60)
    print("    DISTRIBUTED KEY-VALUE STORE CLIENT")
    print("=" * 60)
    print(f"Replication Factor: {args.replication}")
    print(f"Bootstrap Nodes: {args.nodes}")
    print("\nAvailable Commands:")
    print("  put <key> <value>      - Store key-value pair")
    print("  get <key>              - Retrieve value by key")
    print("  delete <key>           - Delete key-value pair")
    print("  add_node <id> <host:port> - Add new node")
    print("  remove_node <id>       - Remove node")
    print("  health <node_id>       - Check node health")
    print("  status                 - Show ring status")
    print("  quit                   - Exit client")
    print("=" * 60)
    
    while True:
        try:
            command = input("\n🎯 Enter command > ").strip().split()
            if not command:
                continue
            
            cmd = command[0].lower()
            
            if cmd == 'put' and len(command) >= 3:
                key = command[1]
                value = ' '.join(command[2:])
                result = client.put(key, value)
                print(json.dumps(result, indent=2))
            
            elif cmd == 'get' and len(command) == 2:
                key = command[1]
                result = client.get(key)
                print(json.dumps(result, indent=2))
            
            elif cmd == 'delete' and len(command) == 2:
                key = command[1]
                result = client.delete(key)
                print(json.dumps(result, indent=2))
            
            elif cmd == 'add_node' and len(command) == 3:
                node_id = command[1]
                host_port = command[2]
                try:
                    host, port = host_port.split(':')
                    message = client.add_node(node_id, host, int(port))
                    print(message)
                except ValueError:
                    print("❌ Invalid format. Use: add_node <node_id> <host:port>")
            
            elif cmd == 'remove_node' and len(command) == 2:
                node_id = command[1]
                message = client.remove_node(node_id)
                print(message)
            
            elif cmd == 'health' and len(command) == 2:
                node_id = command[1]
                result = client.health_check(node_id)
                print(json.dumps(result, indent=2))
            
            elif cmd == 'status':
                status = client.get_ring_status()
                print("📊 Ring Status:")
                print(json.dumps(status, indent=2))
            
            elif cmd in ['quit', 'exit', 'q']:
                print("👋 Goodbye!")
                break
            
            else:
                print("❌ Invalid command. Available commands:")
                print("  put <key> <value>")
                print("  get <key>")
                print("  delete <key>")
                print("  add_node <node_id> <host:port>")
                print("  remove_node <node_id>")
                print("  health <node_id>")
                print("  status")
                print("  quit")
                
        except KeyboardInterrupt:
            print("\n👋 Goodbye!")
            break
        except Exception as e:
            print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()