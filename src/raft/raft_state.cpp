#include "raft/raft_state.hpp"
#include <filesystem>
#include <random>
#include <sstream>

namespace dkv {

// ==================== PersistentState ====================

void PersistentState::save(const std::string& path) const {
    std::ofstream file(path, std::ios::binary);
    if (!file) return;
    
    // Write term
    file.write(reinterpret_cast<const char*>(&current_term), sizeof(current_term));
    
    // Write voted_for
    uint8_t has_vote = voted_for.has_value() ? 1 : 0;
    file.write(reinterpret_cast<const char*>(&has_vote), 1);
    
    if (voted_for) {
        uint32_t len = static_cast<uint32_t>(voted_for->size());
        file.write(reinterpret_cast<const char*>(&len), sizeof(len));
        file.write(voted_for->data(), len);
    }
    
    file.flush();
}

void PersistentState::load(const std::string& path) {
    std::ifstream file(path, std::ios::binary);
    if (!file) return;
    
    // Read term
    file.read(reinterpret_cast<char*>(&current_term), sizeof(current_term));
    
    // Read voted_for
    uint8_t has_vote = 0;
    file.read(reinterpret_cast<char*>(&has_vote), 1);
    
    if (has_vote) {
        uint32_t len = 0;
        file.read(reinterpret_cast<char*>(&len), sizeof(len));
        std::string candidate(len, '\0');
        file.read(candidate.data(), len);
        voted_for = candidate;
    } else {
        voted_for = std::nullopt;
    }
}

// ==================== LeaderState ====================

void LeaderState::reinitialize(const std::vector<std::string>& peers, uint64_t last_log_index) {
    next_index.clear();
    match_index.clear();
    
    for (const auto& peer : peers) {
        next_index[peer] = last_log_index + 1;
        match_index[peer] = 0;
    }
}

// ==================== RaftState ====================

RaftState::RaftState(const std::string& data_dir)
    : data_dir_(data_dir) {
    
    // Create directory if needed
    std::filesystem::create_directories(data_dir);
    
    // Load persistent state
    std::string state_path = data_dir + "/raft_state.dat";
    persistent_.load(state_path);
    
    // Initialize timing
    randomizeElectionTimeout();
    resetElectionTimeout();
    resetHeartbeatTimer();
}

uint64_t RaftState::getCurrentTerm() const {
    std::lock_guard<std::mutex> lock(persistent_mutex_);
    return persistent_.current_term;
}

void RaftState::setCurrentTerm(uint64_t term) {
    std::lock_guard<std::mutex> lock(persistent_mutex_);
    persistent_.current_term = term;
    persistent_.voted_for = std::nullopt;  // Reset vote when term changes
    persist();
}

std::optional<std::string> RaftState::getVotedFor() const {
    std::lock_guard<std::mutex> lock(persistent_mutex_);
    return persistent_.voted_for;
}

void RaftState::setVotedFor(const std::optional<std::string>& candidate_id) {
    std::lock_guard<std::mutex> lock(persistent_mutex_);
    persistent_.voted_for = candidate_id;
    persist();
}

void RaftState::setRole(RaftRole role) {
    role_ = role;
    if (role == RaftRole::RAFT_FOLLOWER) {
        leader_id_.clear();
    }
}

void RaftState::resetElectionTimeout() {
    last_heartbeat_received_ = std::chrono::steady_clock::now();
    randomizeElectionTimeout();
}

bool RaftState::isElectionTimedOut() const {
    auto now = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        now - last_heartbeat_received_
    ).count();
    return elapsed >= election_timeout_ms_;
}

void RaftState::resetHeartbeatTimer() {
    last_heartbeat_sent_ = std::chrono::steady_clock::now();
}

bool RaftState::shouldSendHeartbeat() const {
    auto now = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        now - last_heartbeat_sent_
    ).count();
    return elapsed >= HEARTBEAT_INTERVAL_MS;
}

void RaftState::persist() {
    std::string state_path = data_dir_ + "/raft_state.dat";
    persistent_.save(state_path);
}

void RaftState::randomizeElectionTimeout() {
    static std::random_device rd;
    static std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(150, 300);
    election_timeout_ms_ = dis(gen);
}

} // namespace dkv
