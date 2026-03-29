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
    // Replication opcodes (legacy)
    OP_REPLICATE = 10,
    OP_REPLICATE_ACK = 11,
    OP_JOIN_CLUSTER = 12,
    OP_HEARTBEAT = 13,
    OP_STATUS = 14,
    // Raft consensus opcodes
    OP_REQUEST_VOTE = 20,        // Candidate -> All: request vote
    OP_REQUEST_VOTE_RESP = 21,   // Response to vote request
    OP_APPEND_ENTRIES = 22,      // Leader -> Follower: heartbeat + log replication
    OP_APPEND_ENTRIES_RESP = 23  // Follower -> Leader: response
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

// Node role in cluster (legacy)
enum class NodeRole : uint8_t {
    STANDALONE = 0,
    LEADER = 1,
    FOLLOWER = 2
};

// Raft role
enum class RaftRole : uint8_t {
    RAFT_FOLLOWER = 0,
    RAFT_CANDIDATE = 1,
    RAFT_LEADER = 2
};

// Raft log entry (includes term)
struct RaftLogEntry {
    uint64_t term;
    uint64_t index;
    OpCode op;
    std::string key;
    std::string value;
    
    std::vector<uint8_t> serialize() const;
    static RaftLogEntry deserialize(const std::vector<uint8_t>& data);
};

// RequestVote RPC
struct RequestVote {
    uint64_t term;           // Candidate's term
    std::string candidate_id; // Candidate requesting vote
    uint64_t last_log_index; // Index of candidate's last log entry
    uint64_t last_log_term;  // Term of candidate's last log entry
    
    std::vector<uint8_t> serialize() const;
    static RequestVote deserialize(const std::vector<uint8_t>& data);
};

struct RequestVoteResponse {
    uint64_t term;       // Current term, for candidate to update itself
    bool vote_granted;   // True if candidate received vote
    
    std::vector<uint8_t> serialize() const;
    static RequestVoteResponse deserialize(const std::vector<uint8_t>& data);
};

// AppendEntries RPC (heartbeat when entries is empty)
struct AppendEntries {
    uint64_t term;           // Leader's term
    std::string leader_id;   // So follower can redirect clients
    uint64_t prev_log_index; // Index of log entry immediately preceding new ones
    uint64_t prev_log_term;  // Term of prev_log_index entry
    std::vector<RaftLogEntry> entries; // Log entries to store (empty for heartbeat)
    uint64_t leader_commit;  // Leader's commit index
    
    std::vector<uint8_t> serialize() const;
    static AppendEntries deserialize(const std::vector<uint8_t>& data);
};

struct AppendEntriesResponse {
    uint64_t term;       // Current term, for leader to update itself
    bool success;        // True if follower contained entry matching prev_log
    uint64_t match_index; // Highest log index known to be replicated
    
    std::vector<uint8_t> serialize() const;
    static AppendEntriesResponse deserialize(const std::vector<uint8_t>& data);
};

} // namespace dkv
