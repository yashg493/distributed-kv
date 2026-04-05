#include "shard/sharded_client.hpp"
#include <iostream>

namespace dkv {

ShardedClient::ShardedClient() {}

ShardedClient::~ShardedClient() {
    std::lock_guard<std::mutex> lock(mutex_);
    connections_.clear();
}

bool ShardedClient::parseAddress(const std::string& addr, std::string& host, uint16_t& port) {
    auto colon = addr.find(':');
    if (colon == std::string::npos) {
        return false;
    }
    host = addr.substr(0, colon);
    try {
        port = static_cast<uint16_t>(std::stoi(addr.substr(colon + 1)));
    } catch (...) {
        return false;
    }
    return true;
}

bool ShardedClient::addShard(const std::string& shard_addr) {
    std::string host;
    uint16_t port;
    
    if (!parseAddress(shard_addr, host, port)) {
        std::cerr << "[ShardedClient] Invalid shard address: " << shard_addr << std::endl;
        return false;
    }
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Add to hash ring
    ring_.addNode(shard_addr);
    
    // Create connection
    auto client = std::make_unique<Client>();
    if (!client->connect(host, port)) {
        std::cerr << "[ShardedClient] Failed to connect to shard: " << shard_addr << std::endl;
        ring_.removeNode(shard_addr);
        return false;
    }
    
    connections_[shard_addr] = std::move(client);
    std::cout << "[ShardedClient] Added shard: " << shard_addr << std::endl;
    return true;
}

void ShardedClient::removeShard(const std::string& shard_addr) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    ring_.removeNode(shard_addr);
    connections_.erase(shard_addr);
}

bool ShardedClient::initialize(const std::vector<std::string>& shards) {
    for (const auto& shard : shards) {
        if (!addShard(shard)) {
            return false;
        }
    }
    return !shards.empty();
}

Client* ShardedClient::getConnection(const std::string& shard_addr) {
    // Note: caller must hold mutex_
    auto it = connections_.find(shard_addr);
    if (it == connections_.end()) {
        return nullptr;
    }
    
    Client* client = it->second.get();
    
    // Check if still connected, reconnect if needed
    if (!client->isConnected()) {
        std::string host;
        uint16_t port;
        if (parseAddress(shard_addr, host, port)) {
            client->connect(host, port);
        }
    }
    
    return client->isConnected() ? client : nullptr;
}

bool ShardedClient::put(const std::string& key, const std::string& value) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    std::string shard = ring_.getNode(key);
    if (shard.empty()) {
        std::cerr << "[ShardedClient] No shards available" << std::endl;
        return false;
    }
    
    Client* client = getConnection(shard);
    if (!client) {
        std::cerr << "[ShardedClient] Cannot connect to shard: " << shard << std::endl;
        return false;
    }
    
    return client->put(key, value);
}

std::optional<std::string> ShardedClient::get(const std::string& key) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    std::string shard = ring_.getNode(key);
    if (shard.empty()) {
        return std::nullopt;
    }
    
    Client* client = getConnection(shard);
    if (!client) {
        return std::nullopt;
    }
    
    return client->get(key);
}

bool ShardedClient::del(const std::string& key) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    std::string shard = ring_.getNode(key);
    if (shard.empty()) {
        return false;
    }
    
    Client* client = getConnection(shard);
    if (!client) {
        return false;
    }
    
    return client->del(key);
}

bool ShardedClient::ping(const std::string& shard_addr) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    Client* client = getConnection(shard_addr);
    if (!client) {
        return false;
    }
    
    return client->ping();
}

bool ShardedClient::pingAll() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    for (auto& [addr, client] : connections_) {
        if (!client->isConnected()) {
            std::string host;
            uint16_t port;
            if (parseAddress(addr, host, port)) {
                client->connect(host, port);
            }
        }
        if (!client->ping()) {
            return false;
        }
    }
    return true;
}

std::string ShardedClient::getShardForKey(const std::string& key) const {
    return ring_.getNode(key);
}

std::vector<std::string> ShardedClient::getShards() const {
    return ring_.getNodes();
}

size_t ShardedClient::shardCount() const {
    return ring_.size();
}

} // namespace dkv
