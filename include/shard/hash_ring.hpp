#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <map>
#include <set>
#include <mutex>

namespace dkv {

/**
 * HashRing - Consistent hashing implementation for key distribution.
 * 
 * Uses virtual nodes for better distribution of keys across physical nodes.
 * Hash positions are uint32_t (0 to 2^32-1).
 */
class HashRing {
public:
    explicit HashRing(int virtual_nodes = 150);
    
    // Add a physical node to the ring
    void addNode(const std::string& node_id);
    
    // Remove a physical node from the ring
    void removeNode(const std::string& node_id);
    
    // Get the node responsible for a key
    std::string getNode(const std::string& key) const;
    
    // Get all physical nodes
    std::vector<std::string> getNodes() const;
    
    // Get number of physical nodes
    size_t size() const;
    
    // Check if ring is empty
    bool empty() const;
    
    // Clear all nodes
    void clear();
    
    // Hash a key to a position on the ring
    uint32_t hash(const std::string& key) const;

private:
    int virtual_nodes_;                        // Number of virtual nodes per physical node
    std::map<uint32_t, std::string> ring_;     // position -> node_id
    std::set<std::string> physical_nodes_;     // Set of physical node IDs
    mutable std::mutex mutex_;
    
    // Generate virtual node key
    std::string virtualNodeKey(const std::string& node_id, int index) const;
};

} // namespace dkv
