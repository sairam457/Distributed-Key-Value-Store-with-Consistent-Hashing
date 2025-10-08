import socket
import threading
import json
import time

class KeyValueStore:
    def __init__(self):
        self.data = {}
        self.lock = threading.RLock()
    
    def put(self, key, value, timestamp=None):
        """Store a key-value pair with versioning"""
        with self.lock:
            current_version = self.data.get(key, {}).get('version', 0) + 1
            self.data[key] = {
                'value': value,
                'timestamp': timestamp or time.time(),
                'version': current_version
            }
            return self.data[key]
    
    def get(self, key):
        """Retrieve a key-value pair"""
        with self.lock:
            return self.data.get(key)
    
    def delete(self, key):
        """Delete a key-value pair"""
        with self.lock:
            if key in self.data:
                del self.data[key]
                return True
            return False
    
    def get_all_keys(self):
        """Get all keys in the store"""
        with self.lock:
            return list(self.data.keys())
    
    def get_stats(self):
        """Get store statistics"""
        with self.lock:
            return {
                'total_keys': len(self.data),
                'keys': list(self.data.keys())
            }

class KVServer:
    def __init__(self, host='localhost', port=8000, ring_manager=None, replication_factor=2):
        self.host = host
        self.port = port
        self.node_id = f"{host}:{port}"
        self.ring_manager = ring_manager
        self.replication_factor = replication_factor
        self.store = KeyValueStore()
        self.running = False
        self.server_socket = None
        
    def start(self):
        """Start the server and register in ring"""
        # Register this node in the ring
        if self.ring_manager:
            self.ring_manager.register_node(self.node_id, self.host, self.port)
        
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        
        self.running = True
        print(f"✅ KV Server {self.node_id} started (Replication: {self.replication_factor})")
        
        while self.running:
            try:
                client_socket, address = self.server_socket.accept()
                client_thread = threading.Thread(
                    target=self.handle_client, 
                    args=(client_socket, address),
                    name=f"ClientHandler-{address}"
                )
                client_thread.daemon = True
                client_thread.start()
            except Exception as e:
                if self.running:
                    print(f"❌ Server error: {e}")
    
    def handle_client(self, client_socket, address):
        """Handle client connection"""
        try:
            data = client_socket.recv(4096).decode('utf-8')
            if not data:
                return
                
            request = json.loads(data)
            response = self.process_request(request)
            
            client_socket.send(json.dumps(response).encode('utf-8'))
        except json.JSONDecodeError as e:
            error_response = {'status': 'error', 'message': f'Invalid JSON: {str(e)}'}
            client_socket.send(json.dumps(error_response).encode('utf-8'))
        except Exception as e:
            error_response = {'status': 'error', 'message': str(e)}
            client_socket.send(json.dumps(error_response).encode('utf-8'))
        finally:
            client_socket.close()
    
    def process_request(self, request):
        """Process different types of requests"""
        action = request.get('action')
        
        try:
            if action == 'PUT':
                return self.handle_put(request)
            elif action == 'GET':
                return self.handle_get(request)
            elif action == 'DELETE':
                return self.handle_delete(request)
            elif action == 'REPLICATE':
                return self.handle_replicate(request)
            elif action == 'HEALTH':
                return self.handle_health()
            elif action == 'RING_INFO':
                return self.handle_ring_info()
            elif action == 'STATS':
                return self.handle_stats()
            else:
                return {'status': 'error', 'message': f'Unknown action: {action}'}
        except Exception as e:
            return {'status': 'error', 'message': f'Processing error: {str(e)}'}
    
    def handle_put(self, request):
        """Handle PUT request with replication"""
        key = request['key']
        value = request['value']
        is_replication = request.get('is_replication', False)
        
        # Store locally
        stored_data = self.store.put(key, value)
        
        # Replicate to other nodes if this is not a replication request
        if not is_replication and self.ring_manager:
            responsible_nodes = self.ring_manager.get_responsible_nodes(key)
            
            # Replicate to other responsible nodes (excluding self)
            replication_count = 0
            for node_id in responsible_nodes:
                if node_id != self.node_id:
                    if self.replicate_data(node_id, key, value):
                        replication_count += 1
            
            return {
                'status': 'success', 
                'message': f'Key {key} stored and replicated to {replication_count} nodes',
                'node': self.node_id,
                'timestamp': stored_data['timestamp'],
                'version': stored_data['version'],
                'replicated_to': replication_count
            }
        else:
            return {
                'status': 'success', 
                'message': f'Key {key} stored',
                'node': self.node_id,
                'timestamp': stored_data['timestamp'],
                'version': stored_data['version']
            }
    
    def handle_get(self, request):
        """Handle GET request"""
        key = request['key']
        result = self.store.get(key)
        
        if result:
            return {
                'status': 'success', 
                'value': result['value'],
                'node': self.node_id,
                'timestamp': result['timestamp'],
                'version': result['version']
            }
        else:
            return {'status': 'error', 'message': f'Key {key} not found on {self.node_id}'}
    
    def handle_delete(self, request):
        """Handle DELETE request"""
        key = request['key']
        is_replication = request.get('is_replication', False)
        success = self.store.delete(key)
        
        if success:
            # Replicate delete to other nodes if not a replication request
            if not is_replication and self.ring_manager:
                responsible_nodes = self.ring_manager.get_responsible_nodes(key)
                for node_id in responsible_nodes:
                    if node_id != self.node_id:
                        self.replicate_delete(node_id, key)
            
            return {'status': 'success', 'message': f'Key {key} deleted'}
        else:
            return {'status': 'error', 'message': f'Key {key} not found'}
    
    def handle_replicate(self, request):
        """Handle replication requests from other nodes"""
        key = request['key']
        value = request['value']
        
        # Store without further replication to avoid loops
        stored_data = self.store.put(key, value)
        
        return {
            'status': 'success', 
            'message': f'Key {key} replicated',
            'node': self.node_id
        }
    
    def handle_health(self):
        """Return health status"""
        return {
            'status': 'healthy', 
            'node_id': self.node_id,
            'keys_count': len(self.store.get_all_keys())
        }
    
    def handle_ring_info(self):
        """Return ring information"""
        if self.ring_manager:
            return {
                'status': 'success',
                'ring_info': self.ring_manager.get_ring_info()
            }
        else:
            return {'status': 'error', 'message': 'Ring manager not available'}
    
    def handle_stats(self):
        """Return node statistics"""
        stats = self.store.get_stats()
        stats['node_id'] = self.node_id
        stats['status'] = 'success'
        return stats
    
    def replicate_data(self, target_node_id, key, value):
        """Replicate data to another node"""
        try:
            node_address = self.ring_manager.get_node_address(target_node_id)
            if node_address:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                    sock.settimeout(2)
                    sock.connect(node_address)
                    request = {
                        'action': 'REPLICATE',
                        'key': key,
                        'value': value,
                        'is_replication': True
                    }
                    sock.send(json.dumps(request).encode('utf-8'))
                    response = sock.recv(1024).decode('utf-8')
                    result = json.loads(response)
                    return result.get('status') == 'success'
        except Exception as e:
            print(f"❌ Replication to {target_node_id} failed: {e}")
        return False
    
    def replicate_delete(self, target_node_id, key):
        """Replicate delete operation to another node"""
        try:
            node_address = self.ring_manager.get_node_address(target_node_id)
            if node_address:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                    sock.settimeout(2)
                    sock.connect(node_address)
                    request = {
                        'action': 'DELETE',
                        'key': key,
                        'is_replication': True
                    }
                    sock.send(json.dumps(request).encode('utf-8'))
                    response = sock.recv(1024).decode('utf-8')
                    result = json.loads(response)
                    return result.get('status') == 'success'
        except Exception as e:
            print(f"❌ Delete replication to {target_node_id} failed: {e}")
        return False
    
    def stop(self):
        """Stop the server"""
        self.running = False
        if self.ring_manager:
            self.ring_manager.unregister_node(self.node_id)
        if self.server_socket:
            self.server_socket.close()
        print(f"🛑 Server {self.node_id} stopped")