import json
import socket
from typing import Dict, Tuple, List

class RingManager:
    def __init__(self, replication_factor: int = 2, virtual_nodes: int = 3):
        from .consistent_hashing import ConsistentHashing
        self.consistent_hashing = ConsistentHashing(virtual_nodes)
        self.replication_factor = replication_factor
        self.nodes: Dict = {}
    
    def register_node(self, node_id: str, host: str, port: int):
        """Register a node in the ring"""
        self.consistent_hashing.add_node(node_id)
        self.nodes[node_id] = {
            'host': host, 
            'port': port, 
            'status': 'active',
            'last_seen': self._get_current_timestamp()
        }
        print(f"Node {node_id} registered in ring")
    
    def unregister_node(self, node_id: str):
        """Remove a node from the ring"""
        self.consistent_hashing.remove_node(node_id)
        if node_id in self.nodes:
            self.nodes[node_id]['status'] = 'inactive'
            print(f"Node {node_id} unregistered from ring")
    
    def get_node_address(self, node_id: str) -> Tuple:
        """Get network address for a node"""
        if node_id in self.nodes and self.nodes[node_id]['status'] == 'active':
            node = self.nodes[node_id]
            return (node['host'], node['port'])
        return None
    
    def get_responsible_nodes(self, key: str) -> List[str]:
        """Get all nodes responsible for a key (primary + replicas)"""
        return self.consistent_hashing.get_replication_nodes(key, self.replication_factor)
    
    def get_ring_info(self) -> Dict:
        """Get complete ring information"""
        return {
            'ring_status': self.consistent_hashing.get_ring_status(),
            'nodes': self.nodes,
            'replication_factor': self.replication_factor
        }
    
    def _get_current_timestamp(self) -> float:
        """Get current timestamp"""
        import time
        return time.time()