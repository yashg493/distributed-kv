#include "shard/hash_ring.hpp"
#include <algorithm>

namespace dkv {

// MurmurHash3 finalizer for 32-bit hash
static uint32_t murmur3_32(const std::string& key, uint32_t seed = 0) {
    const uint8_t* data = reinterpret_cast<const uint8_t*>(key.data());
    const int len = static_cast<int>(key.size());
    const int nblocks = len / 4;
    
    uint32_t h1 = seed;
    const uint32_t c1 = 0xcc9e2d51;
    const uint32_t c2 = 0x1b873593;
    
    // Body
    const uint32_t* blocks = reinterpret_cast<const uint32_t*>(data);
    for (int i = 0; i < nblocks; i++) {
        uint32_t k1 = blocks[i];
        
        k1 *= c1;
        k1 = (k1 << 15) | (k1 >> 17);
        k1 *= c2;
        
        h1 ^= k1;
        h1 = (h1 << 13) | (h1 >> 19);
        h1 = h1 * 5 + 0xe6546b64;
    }
    
    // Tail
    const uint8_t* tail = data + nblocks * 4;
    uint32_t k1 = 0;
    
    switch (len & 3) {
        case 3: k1 ^= tail[2] << 16; [[fallthrough]];
        case 2: k1 ^= tail[1] << 8;  [[fallthrough]];
        case 1: k1 ^= tail[0];
                k1 *= c1;
                k1 = (k1 << 15) | (k1 >> 17);
                k1 *= c2;
                h1 ^= k1;
    }
    
    // Finalization
    h1 ^= len;
    h1 ^= h1 >> 16;
    h1 *= 0x85ebca6b;
    h1 ^= h1 >> 13;
    h1 *= 0xc2b2ae35;
    h1 ^= h1 >> 16;
    
    return h1;
}

HashRing::HashRing(int virtual_nodes) 
    : virtual_nodes_(virtual_nodes) {}

void HashRing::addNode(const std::string& node_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (physical_nodes_.count(node_id) > 0) {
        return;  // Already exists
    }
    
    physical_nodes_.insert(node_id);
    
    // Add virtual nodes
    for (int i = 0; i < virtual_nodes_; ++i) {
        std::string vnode_key = virtualNodeKey(node_id, i);
        uint32_t pos = murmur3_32(vnode_key);
        ring_[pos] = node_id;
    }
}

void HashRing::removeNode(const std::string& node_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (physical_nodes_.count(node_id) == 0) {
        return;  // Not found
    }
    
    physical_nodes_.erase(node_id);
    
    // Remove virtual nodes
    for (int i = 0; i < virtual_nodes_; ++i) {
        std::string vnode_key = virtualNodeKey(node_id, i);
        uint32_t pos = murmur3_32(vnode_key);
        ring_.erase(pos);
    }
}

std::string HashRing::getNode(const std::string& key) const {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (ring_.empty()) {
        return "";
    }
    
    uint32_t pos = murmur3_32(key);
    
    // Find first node with position >= key's hash
    auto it = ring_.lower_bound(pos);
    
    // If we went past the end, wrap around to the first node
    if (it == ring_.end()) {
        it = ring_.begin();
    }
    
    return it->second;
}

std::vector<std::string> HashRing::getNodes() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return std::vector<std::string>(physical_nodes_.begin(), physical_nodes_.end());
}

size_t HashRing::size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return physical_nodes_.size();
}

bool HashRing::empty() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return physical_nodes_.empty();
}

void HashRing::clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    ring_.clear();
    physical_nodes_.clear();
}

uint32_t HashRing::hash(const std::string& key) const {
    return murmur3_32(key);
}

std::string HashRing::virtualNodeKey(const std::string& node_id, int index) const {
    return node_id + "#" + std::to_string(index);
}

} // namespace dkv
