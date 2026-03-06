#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace dkv {

enum class OpCode : uint8_t {
    OP_PUT = 1,
    OP_GET = 2,
    OP_DELETE = 3,
    OP_PING = 4,
    // Replication opcodes
    OP_REPLICATE = 10,      // Leader -> Follower: replicate operation
    OP_REPLICATE_ACK = 11,  // Follower -> Leader: acknowledge replication
    OP_JOIN_CLUSTER = 12,   // Follower -> Leader: request to join
    OP_HEARTBEAT = 13,      // Leader -> Follower: health check
    OP_STATUS = 14          // Client -> Server: get node status
};

enum class StatusCode : uint8_t {
    STATUS_OK = 0,
    STATUS_NOT_FOUND = 1,
    STATUS_ERROR = 2
};

struct Request {
    OpCode op;
    std::string key;
    std::string value;
    
    std::vector<uint8_t> serialize() const;
    static Request deserialize(const std::vector<uint8_t>& data);
};

struct Response {
    StatusCode status;
    std::string value;
    std::string error;
    
    std::vector<uint8_t> serialize() const;
    static Response deserialize(const std::vector<uint8_t>& data);
};

// Replication entry for leader-follower sync
struct ReplicationEntry {
    uint64_t sequence_num;
    OpCode op;
    std::string key;
    std::string value;
    uint64_t timestamp;
    
    std::vector<uint8_t> serialize() const;
    static ReplicationEntry deserialize(const std::vector<uint8_t>& data);
};

// Node role in cluster
enum class NodeRole : uint8_t {
    STANDALONE = 0,
    LEADER = 1,
    FOLLOWER = 2
};

} // namespace dkv
