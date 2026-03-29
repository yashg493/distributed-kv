#pragma once

#include <cstdint>
#include <string>
#include <optional>
#include <chrono>
#include <fstream>
#include <mutex>
#include <map>
#include <vector>
#include "network/protocol.hpp"

namespace dkv {

/**
 * Persistent state on all servers (must be updated on stable storage before responding to RPCs)
 */
struct PersistentState {
    uint64_t current_term = 0;              // Latest term server has seen
    std::optional<std::string> voted_for;   // CandidateId that received vote in current term
    
    void save(const std::string& path) const;
    void load(const std::string& path);
};

/**
 * Volatile state on all servers
 */
struct VolatileState {
    uint64_t commit_index = 0;   // Index of highest log entry known to be committed
    uint64_t last_applied = 0;  // Index of highest log entry applied to state machine
};

/**
 * Volatile state on leaders (reinitialized after election)
 */
struct LeaderState {
    std::map<std::string, uint64_t> next_index;   // For each peer: index of next log entry to send
    std::map<std::string, uint64_t> match_index;  // For each peer: index of highest log entry known to be replicated
    
    void reinitialize(const std::vector<std::string>& peers, uint64_t last_log_index);
};

/**
 * Complete Raft state for a node
 */
class RaftState {
public:
    explicit RaftState(const std::string& data_dir);
    
    // Persistent state access
    uint64_t getCurrentTerm() const;
    void setCurrentTerm(uint64_t term);
    std::optional<std::string> getVotedFor() const;
    void setVotedFor(const std::optional<std::string>& candidate_id);
    
    // Role management
    RaftRole getRole() const { return role_; }
    void setRole(RaftRole role);
    
    // Leader ID (who we think is the leader)
    std::string getLeaderId() const { return leader_id_; }
    void setLeaderId(const std::string& id) { leader_id_ = id; }
    
    // Node ID
    std::string getNodeId() const { return node_id_; }
    void setNodeId(const std::string& id) { node_id_ = id; }
    
    // Volatile state
    VolatileState& volatile_state() { return volatile_; }
    const VolatileState& volatile_state() const { return volatile_; }
    
    // Leader state
    LeaderState& leader_state() { return leader_; }
    const LeaderState& leader_state() const { return leader_; }
    
    // Election timing
    void resetElectionTimeout();
    bool isElectionTimedOut() const;
    int getElectionTimeoutMs() const { return election_timeout_ms_; }
    
    // Heartbeat timing
    void resetHeartbeatTimer();
    bool shouldSendHeartbeat() const;
    
    // Persist to disk
    void persist();

private:
    std::string data_dir_;
    std::string node_id_;
    
    // Persistent state
    PersistentState persistent_;
    mutable std::mutex persistent_mutex_;
    
    // Volatile state
    VolatileState volatile_;
    LeaderState leader_;
    
    // Role
    RaftRole role_ = RaftRole::RAFT_FOLLOWER;
    std::string leader_id_;
    
    // Timing
    std::chrono::steady_clock::time_point last_heartbeat_received_;
    std::chrono::steady_clock::time_point last_heartbeat_sent_;
    int election_timeout_ms_ = 300;  // Randomized 150-300ms
    static constexpr int HEARTBEAT_INTERVAL_MS = 50;  // Send heartbeat every 50ms
    
    void randomizeElectionTimeout();
};

} // namespace dkv
