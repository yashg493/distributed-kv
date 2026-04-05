#pragma once

#include <string>
#include <vector>
#include <map>
#include <memory>
#include <optional>
#include "shard/hash_ring.hpp"
#include "network/client.hpp"

namespace dkv {

/**
 * ShardedClient - A client that routes requests to the correct shard
 * using consistent hashing.
 * 
 * Each shard is a separate server instance. The client maintains
 * connections to all shards and routes each key to the correct one.
 */
class ShardedClient {
public:
    ShardedClient();
    ~ShardedClient();

    ShardedClient(const ShardedClient&) = delete;
    ShardedClient& operator=(const ShardedClient&) = delete;

    // Add a shard (format: "host:port")
    bool addShard(const std::string& shard_addr);
    
    // Remove a shard
    void removeShard(const std::string& shard_addr);
    
    // Initialize with a list of shards
    bool initialize(const std::vector<std::string>& shards);
    
    // KV operations - routed to correct shard
    bool put(const std::string& key, const std::string& value);
    std::optional<std::string> get(const std::string& key);
    bool del(const std::string& key);
    
    // Utility
    bool ping(const std::string& shard_addr);  // Ping a specific shard
    bool pingAll();  // Ping all shards
    
    // Get the shard responsible for a key
    std::string getShardForKey(const std::string& key) const;
    
    // Get all shards
    std::vector<std::string> getShards() const;
    
    // Get number of shards
    size_t shardCount() const;

private:
    // Get or create connection to a shard
    Client* getConnection(const std::string& shard_addr);
    
    // Parse host:port
    static bool parseAddress(const std::string& addr, std::string& host, uint16_t& port);

    HashRing ring_;
    std::map<std::string, std::unique_ptr<Client>> connections_;
    std::mutex mutex_;
};

} // namespace dkv
