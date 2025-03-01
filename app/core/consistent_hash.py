import hashlib
from bisect import insort_left, bisect_left
from typing import List, Dict

class ConsistentHash:
    def __init__(self, nodes: List[str] = [], virtual_nodes: int = 100):
        self.virtual_nodes = virtual_nodes
        self.hash_ring: Dict[int, str] = {}  # Map hash -> node
        self.sorted_keys: List[int] = []  # Sorted hash values (kept sorted)

        # Add all initial nodes
        for node in nodes:
            self.add_node(node)

    def _hash(self, key: str) -> int:
        """Generate a consistent hash for a given key."""
        return int(hashlib.sha256(key.encode()).hexdigest(), 16) % (2**32)

    def add_node(self, node: str) -> None:
        """
        Add a new node in O(N) using binary insertion.
        - Generates virtual nodes.
        - Finds insertion index using `bisect.insort_left()`.
        - Updates `hash_ring` mapping.
        """
        for i in range(self.virtual_nodes):
            key = f"{node}-{i}"
            hash_value = self._hash(key)

            if hash_value in self.hash_ring:
                continue  # Avoid duplicate keys

            # Insert into sorted_keys in the correct position
            insort_left(self.sorted_keys, hash_value)

            # Update hash ring with the new virtual node mapping
            self.hash_ring[hash_value] = node

    def remove_node(self, node: str) -> None:
        """Remove a node and its virtual nodes in O(N)."""
        remove_keys = [h for h, n in self.hash_ring.items() if n == node]

        for key in remove_keys:
            del self.hash_ring[key]

        # Remove keys in O(N) without re-sorting
        self.sorted_keys = [h for h in self.sorted_keys if h not in remove_keys]

    def get_node(self, key: str) -> str:
        """Get the node responsible for a given key using binary search (O(log N))."""
        if not self.sorted_keys:
            return None  # No nodes available

        hash_value = self._hash(key)
        idx = bisect_left(self.sorted_keys, hash_value)

        if idx == len(self.sorted_keys):  # Wrap around
            idx = 0

        return self.hash_ring[self.sorted_keys[idx]]