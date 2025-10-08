import hashlib
import bisect
from typing import List, Dict, Optional

class ConsistentHashing:
    def __init__(self, virtual_nodes: int = 3):
        self.virtual_nodes = virtual_nodes
        self.ring = {}  # hash -> node
        self.sorted_hashes = []
        self.nodes = set()
    
    def _hash(self, key: str) -> int:
        """Generate a 32-bit hash for a key"""
        return int(hashlib.md5(key.encode()).hexdigest(), 16) % (2**32)
    
    def add_node(self, node_id: str):
        """Add a node to the consistent hashing ring with virtual nodes"""
        if node_id in self.nodes:
            return
        
        self.nodes.add(node_id)
        
        # Add virtual nodes
        for i in range(self.virtual_nodes):
            virtual_node_id = f"{node_id}-vnode-{i}"
            hash_val = self._hash(virtual_node_id)
            
            # Handle hash collisions
            while hash_val in self.ring:
                virtual_node_id = f"{virtual_node_id}-collision"
                hash_val = self._hash(virtual_node_id)
            
            self.ring[hash_val] = node_id
            bisect.insort(self.sorted_hashes, hash_val)
    
    def remove_node(self, node_id: str):
        """Remove a node and its virtual nodes from the ring"""
        if node_id not in self.nodes:
            return
        
        self.nodes.remove(node_id)
        
        # Remove all virtual nodes for this physical node
        hashes_to_remove = []
        for hash_val, node in self.ring.items():
            if node == node_id:
                hashes_to_remove.append(hash_val)
        
        for hash_val in hashes_to_remove:
            del self.ring[hash_val]
            self.sorted_hashes.remove(hash_val)
    
    def get_node(self, key: str) -> str:
        """Get the node responsible for a given key"""
        if not self.ring:
            raise ValueError("No nodes available in the ring")
        
        key_hash = self._hash(key)
        
        # Find the first node with hash >= key_hash
        idx = bisect.bisect_left(self.sorted_hashes, key_hash)
        
        if idx == len(self.sorted_hashes):
            # Wrap around to the first node
            idx = 0
        
        return self.ring[self.sorted_hashes[idx]]
    
    def get_replication_nodes(self, key: str, replication_factor: int) -> List[str]:
        """Get primary node and replication nodes for a key"""
        if replication_factor <= 0:
            raise ValueError("Replication factor must be positive")
        
        if not self.ring:
            return []
        
        primary_node = self.get_node(key)
        nodes = [primary_node]
        
        if replication_factor == 1:
            return nodes
        
        # Find the index of primary node's hash
        primary_hashes = [h for h, n in self.ring.items() if n == primary_node]
        if not primary_hashes:
            return nodes
        
        primary_hash = primary_hashes[0]
        primary_idx = self.sorted_hashes.index(primary_hash)
        
        # Get next N-1 nodes for replication
        added_nodes = set([primary_node])
        current_idx = primary_idx
        
        while len(nodes) < replication_factor and len(nodes) < len(self.nodes):
            current_idx = (current_idx + 1) % len(self.sorted_hashes)
            next_node = self.ring[self.sorted_hashes[current_idx]]
            
            if next_node not in added_nodes:
                nodes.append(next_node)
                added_nodes.add(next_node)
        
        return nodes
    
    def get_ring_status(self) -> Dict:
        """Get current status of the ring"""
        return {
            "total_nodes": len(self.nodes),
            "total_virtual_nodes": len(self.ring),
            "sorted_hashes": self.sorted_hashes,
            "ring_mapping": self.ring
        }