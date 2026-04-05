#include "raft/raft_node.hpp"
#include <iostream>
#include <sstream>
#include <algorithm>

namespace dkv {

// Parse peer address "host:port"
static PeerInfo parsePeerAddress(const std::string& addr) {
    PeerInfo peer;
    peer.id = addr;
    auto colon = addr.find(':');
    if (colon != std::string::npos) {
        peer.host = addr.substr(0, colon);
        peer.port = static_cast<uint16_t>(std::stoi(addr.substr(colon + 1)));
    }
    return peer;
}

RaftNode::RaftNode(const std::string& data_dir, uint16_t port,
                   const std::vector<std::string>& peers)
    : port_(port), data_dir_(data_dir), state_(data_dir) {
    
#ifdef _WIN32
    WSADATA wsaData;
    WSAStartup(MAKEWORD(2, 2), &wsaData);
#endif
    
    // Set node ID
    std::string node_id = "127.0.0.1:" + std::to_string(port);
    state_.setNodeId(node_id);
    
    // Parse peers (excluding self)
    for (const auto& addr : peers) {
        if (addr != node_id) {
            peers_.push_back(parsePeerAddress(addr));
        }
    }
    
    // Initialize storage
    store_ = std::make_unique<LSMTree>(data_dir);
    
    // Initialize log with dummy entry at index 0
    RaftLogEntry dummy;
    dummy.term = 0;
    dummy.index = 0;
    dummy.op = OpCode::OP_PING;
    log_.push_back(dummy);
}

RaftNode::~RaftNode() {
    stop();
    
#ifdef _WIN32
    WSACleanup();
#endif
}

void RaftNode::start() {
    if (running_) return;
    running_ = true;
    
    // Create server socket
    server_sock_ = socket(AF_INET, SOCK_STREAM, 0);
    if (server_sock_ == INVALID_SOCK) {
        throw std::runtime_error("Failed to create socket");
    }
    
    int opt = 1;
#ifdef _WIN32
    setsockopt(server_sock_, SOL_SOCKET, SO_REUSEADDR, (const char*)&opt, sizeof(opt));
#else
    setsockopt(server_sock_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
#endif
    
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port_);
    
    if (bind(server_sock_, (sockaddr*)&addr, sizeof(addr)) < 0) {
        CLOSE_SOCKET(server_sock_);
        throw std::runtime_error("Failed to bind to port " + std::to_string(port_));
    }
    
    if (listen(server_sock_, 10) < 0) {
        CLOSE_SOCKET(server_sock_);
        throw std::runtime_error("Failed to listen");
    }
    
    std::cout << "[RAFT] Node " << state_.getNodeId() << " starting as FOLLOWER" << std::endl;
    
    // Start threads
    accept_thread_ = std::thread(&RaftNode::acceptLoop, this);
    raft_thread_ = std::thread(&RaftNode::raftLoop, this);
    peer_thread_ = std::thread(&RaftNode::peerConnectionLoop, this);
}

void RaftNode::stop() {
    if (!running_) return;
    running_ = false;
    
    // Wake up raft loop
    raft_cv_.notify_all();
    
    // Close server socket
    if (server_sock_ != INVALID_SOCK) {
        CLOSE_SOCKET(server_sock_);
        server_sock_ = INVALID_SOCK;
    }
    
    // Disconnect peers
    {
        std::lock_guard<std::mutex> lock(peers_mutex_);
        for (auto& peer : peers_) {
            disconnectPeer(peer);
        }
    }
    
    // Join threads
    if (accept_thread_.joinable()) accept_thread_.join();
    if (raft_thread_.joinable()) raft_thread_.join();
    if (peer_thread_.joinable()) peer_thread_.join();
    
    {
        std::lock_guard<std::mutex> lock(threads_mutex_);
        for (auto& t : client_threads_) {
            if (t.joinable()) t.join();
        }
        client_threads_.clear();
    }
}

// ==================== Main Loops ====================

void RaftNode::acceptLoop() {
    while (running_) {
        sockaddr_in client_addr{};
        socklen_t addr_len = sizeof(client_addr);
        
        SocketType client_sock = accept(server_sock_, (sockaddr*)&client_addr, &addr_len);
        if (client_sock == INVALID_SOCK) {
            if (running_) {
                // Could be a spurious wakeup or error
            }
            continue;
        }
        
        std::lock_guard<std::mutex> lock(threads_mutex_);
        client_threads_.emplace_back(&RaftNode::handleClient, this, client_sock);
    }
}

void RaftNode::raftLoop() {
    // Wait for peer connections to establish before starting elections
    // This gives time for the cluster to form
    std::this_thread::sleep_for(std::chrono::seconds(2));
    state_.resetElectionTimeout();
    
    while (running_) {
        // Sleep for a short interval
        {
            std::unique_lock<std::mutex> lock(raft_mutex_);
            raft_cv_.wait_for(lock, std::chrono::milliseconds(10));
        }
        
        if (!running_) break;
        
        RaftRole role = state_.getRole();
        
        if (role == RaftRole::RAFT_LEADER) {
            // Send heartbeats periodically
            if (state_.shouldSendHeartbeat()) {
                sendHeartbeats();
                state_.resetHeartbeatTimer();
            }
        } else {
            // Check for election timeout
            if (state_.isElectionTimedOut()) {
                // Only start election if we have peer connections
                int connected_peers = 0;
                {
                    std::lock_guard<std::mutex> lock(peers_mutex_);
                    for (const auto& peer : peers_) {
                        if (peer.connected) connected_peers++;
                    }
                }
                
                // Need at least one peer connected to have a chance at majority
                if (connected_peers > 0 || peers_.size() == 0) {
                    startElection();
                } else {
                    // Reset timeout and try again later
                    state_.resetElectionTimeout();
                }
            }
        }
        
        // Apply committed entries
        applyCommittedEntries();
    }
}

void RaftNode::peerConnectionLoop() {
    while (running_) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        
        std::lock_guard<std::mutex> lock(peers_mutex_);
        for (auto& peer : peers_) {
            if (!peer.connected) {
                connectToPeer(peer);
            }
        }
    }
}

// ==================== Client Handling ====================

void RaftNode::handleClient(SocketType client_sock) {
    while (running_) {
        auto msg = recvRawMessage(client_sock);
        if (msg.empty()) break;
        
        try {
            Request req = Request::deserialize(msg);
            
            // Handle Raft RPCs
            if (req.op == OpCode::OP_REQUEST_VOTE) {
                RequestVote rv = RequestVote::deserialize(
                    std::vector<uint8_t>(req.value.begin(), req.value.end())
                );
                auto resp = handleRequestVote(rv);
                auto resp_data = resp.serialize();
                
                Request reply;
                reply.op = OpCode::OP_REQUEST_VOTE_RESP;
                reply.value = std::string(resp_data.begin(), resp_data.end());
                sendRawMessage(client_sock, reply.serialize());
                continue;
            }
            
            if (req.op == OpCode::OP_APPEND_ENTRIES) {
                AppendEntries ae = AppendEntries::deserialize(
                    std::vector<uint8_t>(req.value.begin(), req.value.end())
                );
                auto resp = handleAppendEntries(ae);
                auto resp_data = resp.serialize();
                
                Request reply;
                reply.op = OpCode::OP_APPEND_ENTRIES_RESP;
                reply.value = std::string(resp_data.begin(), resp_data.end());
                sendRawMessage(client_sock, reply.serialize());
                continue;
            }
            
            // Handle client request
            Response resp = processClientRequest(req);
            sendRawMessage(client_sock, resp.serialize());
            
        } catch (const std::exception& e) {
            Response resp{StatusCode::STATUS_ERROR, "", e.what()};
            sendRawMessage(client_sock, resp.serialize());
        }
    }
    
    CLOSE_SOCKET(client_sock);
}

Response RaftNode::processClientRequest(const Request& req) {
    Response resp;
    resp.status = StatusCode::STATUS_OK;
    
    switch (req.op) {
        case OpCode::OP_PUT:
        case OpCode::OP_DELETE: {
            // Write operations must go through leader
            if (state_.getRole() != RaftRole::RAFT_LEADER) {
                resp.status = StatusCode::STATUS_ERROR;
                resp.error = "Not leader. Leader: " + state_.getLeaderId();
                return resp;
            }
            
            // Append to log
            uint64_t index = appendLog(req.op, req.key, req.value);
            
            // Replicate to followers (simplified: just send immediately)
            sendHeartbeats();
            
            // Wait for commit (simplified: immediate for now)
            // In real implementation, we'd wait for majority and then respond
            
            // Apply to state machine
            if (req.op == OpCode::OP_PUT) {
                store_->put(req.key, req.value);
            } else {
                store_->del(req.key);
            }
            break;
        }
        
        case OpCode::OP_GET: {
            // Reads can be served by any node (stale reads possible)
            auto value = store_->get(req.key);
            if (value) {
                resp.value = *value;
            } else {
                resp.status = StatusCode::STATUS_NOT_FOUND;
            }
            break;
        }
        
        case OpCode::OP_PING:
            resp.value = "PONG";
            break;
            
        case OpCode::OP_STATUS:
            return buildStatusResponse();
            
        default:
            resp.status = StatusCode::STATUS_ERROR;
            resp.error = "Unknown operation";
    }
    
    return resp;
}

Response RaftNode::buildStatusResponse() const {
    Response resp;
    resp.status = StatusCode::STATUS_OK;
    
    std::stringstream ss;
    ss << "node:" << state_.getNodeId() << "\n";
    ss << "role:";
    switch (state_.getRole()) {
        case RaftRole::RAFT_LEADER: ss << "leader"; break;
        case RaftRole::RAFT_CANDIDATE: ss << "candidate"; break;
        case RaftRole::RAFT_FOLLOWER: ss << "follower"; break;
    }
    ss << "\n";
    ss << "term:" << state_.getCurrentTerm() << "\n";
    ss << "leader:" << state_.getLeaderId() << "\n";
    ss << "log_size:" << getLastLogIndex() << "\n";
    ss << "commit_index:" << state_.volatile_state().commit_index << "\n";
    
    int connected = 0;
    {
        std::lock_guard<std::mutex> lock(const_cast<std::mutex&>(peers_mutex_));
        for (const auto& p : peers_) {
            if (p.connected) connected++;
        }
    }
    ss << "peers:" << peers_.size() << " (connected:" << connected << ")\n";
    
    resp.value = ss.str();
    return resp;
}

// ==================== Leader Election ====================

void RaftNode::startElection() {
    std::lock_guard<std::mutex> lock(election_mutex_);
    
    becomeCandidate();
    
    uint64_t term = state_.getCurrentTerm();
    std::cout << "[RAFT] Node " << state_.getNodeId() << " starting election for term " << term << std::endl;
    
    // Vote for self
    votes_received_ = 1;
    state_.setVotedFor(state_.getNodeId());
    
    // Request votes from peers
    std::lock_guard<std::mutex> peers_lock(peers_mutex_);
    for (auto& peer : peers_) {
        if (peer.connected) {
            requestVoteFromPeer(peer);
        }
    }
    
    // Check if we won
    int majority = (static_cast<int>(peers_.size()) + 1) / 2 + 1;
    if (votes_received_ >= majority) {
        becomeLeader();
    }
}

void RaftNode::requestVoteFromPeer(PeerInfo& peer) {
    RequestVote rv;
    rv.term = state_.getCurrentTerm();
    rv.candidate_id = state_.getNodeId();
    rv.last_log_index = getLastLogIndex();
    rv.last_log_term = getLastLogTerm();
    
    auto rv_data = rv.serialize();
    
    Request req;
    req.op = OpCode::OP_REQUEST_VOTE;
    req.value = std::string(rv_data.begin(), rv_data.end());
    
    if (!sendRawMessage(peer.socket, req.serialize())) {
        peer.connected = false;
        return;
    }
    
    auto resp_data = recvRawMessage(peer.socket);
    if (resp_data.empty()) {
        peer.connected = false;
        return;
    }
    
    try {
        Request reply = Request::deserialize(resp_data);
        if (reply.op == OpCode::OP_REQUEST_VOTE_RESP) {
            auto rvr = RequestVoteResponse::deserialize(
                std::vector<uint8_t>(reply.value.begin(), reply.value.end())
            );
            
            if (rvr.term > state_.getCurrentTerm()) {
                becomeFollower(rvr.term);
                return;
            }
            
            if (rvr.vote_granted && state_.getRole() == RaftRole::RAFT_CANDIDATE) {
                votes_received_++;
                std::cout << "[RAFT] Received vote from " << peer.id << " (total: " << votes_received_.load() << ")" << std::endl;
            }
        }
    } catch (...) {}
}

RequestVoteResponse RaftNode::handleRequestVote(const RequestVote& rv) {
    RequestVoteResponse resp;
    resp.term = state_.getCurrentTerm();
    resp.vote_granted = false;
    
    // Reply false if term < currentTerm
    if (rv.term < state_.getCurrentTerm()) {
        return resp;
    }
    
    // If RPC term > currentTerm, update and convert to follower
    if (rv.term > state_.getCurrentTerm()) {
        becomeFollower(rv.term);
    }
    
    // Check if we can vote for this candidate
    auto voted_for = state_.getVotedFor();
    bool can_vote = !voted_for.has_value() || voted_for.value() == rv.candidate_id;
    
    // Check if candidate's log is at least as up-to-date as ours
    bool log_ok = (rv.last_log_term > getLastLogTerm()) ||
                  (rv.last_log_term == getLastLogTerm() && rv.last_log_index >= getLastLogIndex());
    
    if (can_vote && log_ok) {
        resp.vote_granted = true;
        state_.setVotedFor(rv.candidate_id);
        state_.resetElectionTimeout();
        std::cout << "[RAFT] Voting for " << rv.candidate_id << " in term " << rv.term << std::endl;
    }
    
    resp.term = state_.getCurrentTerm();
    return resp;
}

// ==================== Heartbeats & AppendEntries ====================

void RaftNode::sendHeartbeats() {
    std::lock_guard<std::mutex> lock(peers_mutex_);
    
    for (auto& peer : peers_) {
        if (peer.connected) {
            sendAppendEntriesToPeer(peer);
        }
    }
}

void RaftNode::sendAppendEntriesToPeer(PeerInfo& peer) {
    AppendEntries ae;
    ae.term = state_.getCurrentTerm();
    ae.leader_id = state_.getNodeId();
    ae.leader_commit = state_.volatile_state().commit_index;
    
    // Get entries to send
    uint64_t next_idx = state_.leader_state().next_index[peer.id];
    ae.prev_log_index = next_idx - 1;
    ae.prev_log_term = (ae.prev_log_index > 0) ? getLogEntry(ae.prev_log_index).term : 0;
    
    // Add entries from next_index onwards
    std::lock_guard<std::mutex> log_lock(log_mutex_);
    for (uint64_t i = next_idx; i <= getLastLogIndex() && ae.entries.size() < 100; ++i) {
        ae.entries.push_back(log_[i]);
    }
    
    auto ae_data = ae.serialize();
    
    Request req;
    req.op = OpCode::OP_APPEND_ENTRIES;
    req.value = std::string(ae_data.begin(), ae_data.end());
    
    if (!sendRawMessage(peer.socket, req.serialize())) {
        peer.connected = false;
        return;
    }
    
    auto resp_data = recvRawMessage(peer.socket);
    if (resp_data.empty()) {
        peer.connected = false;
        return;
    }
    
    try {
        Request reply = Request::deserialize(resp_data);
        if (reply.op == OpCode::OP_APPEND_ENTRIES_RESP) {
            auto aer = AppendEntriesResponse::deserialize(
                std::vector<uint8_t>(reply.value.begin(), reply.value.end())
            );
            
            if (aer.term > state_.getCurrentTerm()) {
                becomeFollower(aer.term);
                return;
            }
            
            if (aer.success) {
                state_.leader_state().next_index[peer.id] = aer.match_index + 1;
                state_.leader_state().match_index[peer.id] = aer.match_index;
                
                // Update commit index
                // Find the highest index replicated on majority
                std::vector<uint64_t> match_indices;
                match_indices.push_back(getLastLogIndex()); // Leader's own
                for (const auto& [_, idx] : state_.leader_state().match_index) {
                    match_indices.push_back(idx);
                }
                std::sort(match_indices.begin(), match_indices.end());
                
                size_t majority_idx = match_indices.size() / 2;
                uint64_t new_commit = match_indices[majority_idx];
                
                if (new_commit > state_.volatile_state().commit_index &&
                    getLogEntry(new_commit).term == state_.getCurrentTerm()) {
                    state_.volatile_state().commit_index = new_commit;
                }
            } else {
                // Decrement next_index and retry
                if (state_.leader_state().next_index[peer.id] > 1) {
                    state_.leader_state().next_index[peer.id]--;
                }
            }
        }
    } catch (...) {}
}

AppendEntriesResponse RaftNode::handleAppendEntries(const AppendEntries& ae) {
    AppendEntriesResponse resp;
    resp.term = state_.getCurrentTerm();
    resp.success = false;
    resp.match_index = 0;
    
    // Reply false if term < currentTerm
    if (ae.term < state_.getCurrentTerm()) {
        return resp;
    }
    
    // Valid leader heartbeat - reset election timeout
    state_.resetElectionTimeout();
    
    // If RPC term >= currentTerm, recognize leader
    if (ae.term >= state_.getCurrentTerm()) {
        if (state_.getRole() != RaftRole::RAFT_FOLLOWER) {
            becomeFollower(ae.term);
        } else if (ae.term > state_.getCurrentTerm()) {
            state_.setCurrentTerm(ae.term);
        }
        state_.setLeaderId(ae.leader_id);
    }
    
    // Check if log contains entry at prev_log_index with prev_log_term
    std::lock_guard<std::mutex> lock(log_mutex_);
    
    if (ae.prev_log_index > 0) {
        if (ae.prev_log_index >= log_.size()) {
            return resp; // We don't have the entry
        }
        if (log_[ae.prev_log_index].term != ae.prev_log_term) {
            return resp; // Term mismatch
        }
    }
    
    // Append entries
    uint64_t idx = ae.prev_log_index + 1;
    for (const auto& entry : ae.entries) {
        if (idx < log_.size()) {
            if (log_[idx].term != entry.term) {
                // Conflict - delete this and all following
                log_.resize(idx);
                log_.push_back(entry);
            }
            // else: entry already exists and matches
        } else {
            log_.push_back(entry);
        }
        idx++;
    }
    
    // Update commit index
    if (ae.leader_commit > state_.volatile_state().commit_index) {
        state_.volatile_state().commit_index = 
            std::min(ae.leader_commit, static_cast<uint64_t>(log_.size() - 1));
    }
    
    resp.success = true;
    resp.match_index = log_.size() - 1;
    resp.term = state_.getCurrentTerm();
    return resp;
}

// ==================== Log Management ====================

uint64_t RaftNode::appendLog(OpCode op, const std::string& key, const std::string& value) {
    std::lock_guard<std::mutex> lock(log_mutex_);
    
    RaftLogEntry entry;
    entry.term = state_.getCurrentTerm();
    entry.index = log_.size();
    entry.op = op;
    entry.key = key;
    entry.value = value;
    
    log_.push_back(entry);
    return entry.index;
}

RaftLogEntry RaftNode::getLogEntry(uint64_t index) const {
    std::lock_guard<std::mutex> lock(log_mutex_);
    if (index < log_.size()) {
        return log_[index];
    }
    return RaftLogEntry{};
}

uint64_t RaftNode::getLastLogIndex() const {
    std::lock_guard<std::mutex> lock(log_mutex_);
    return log_.size() - 1;
}

uint64_t RaftNode::getLastLogTerm() const {
    std::lock_guard<std::mutex> lock(log_mutex_);
    return log_.empty() ? 0 : log_.back().term;
}

void RaftNode::applyCommittedEntries() {
    std::lock_guard<std::mutex> lock(log_mutex_);
    
    while (state_.volatile_state().last_applied < state_.volatile_state().commit_index) {
        state_.volatile_state().last_applied++;
        uint64_t idx = state_.volatile_state().last_applied;
        
        if (idx < log_.size()) {
            const auto& entry = log_[idx];
            if (entry.op == OpCode::OP_PUT) {
                store_->put(entry.key, entry.value);
            } else if (entry.op == OpCode::OP_DELETE) {
                store_->del(entry.key);
            }
        }
    }
}

// ==================== State Transitions ====================

void RaftNode::becomeFollower(uint64_t term) {
    std::cout << "[RAFT] Becoming FOLLOWER for term " << term << std::endl;
    state_.setCurrentTerm(term);
    state_.setRole(RaftRole::RAFT_FOLLOWER);
    state_.resetElectionTimeout();
}

void RaftNode::becomeCandidate() {
    state_.setCurrentTerm(state_.getCurrentTerm() + 1);
    state_.setRole(RaftRole::RAFT_CANDIDATE);
    state_.resetElectionTimeout();
}

void RaftNode::becomeLeader() {
    std::cout << "[RAFT] Becoming LEADER for term " << state_.getCurrentTerm() << std::endl;
    state_.setRole(RaftRole::RAFT_LEADER);
    state_.setLeaderId(state_.getNodeId());
    
    // Initialize leader state
    std::vector<std::string> peer_ids;
    for (const auto& p : peers_) {
        peer_ids.push_back(p.id);
    }
    state_.leader_state().reinitialize(peer_ids, getLastLogIndex());
    
    // Send initial heartbeats
    sendHeartbeats();
    state_.resetHeartbeatTimer();
}

// ==================== Network Helpers ====================

bool RaftNode::connectToPeer(PeerInfo& peer) {
    if (peer.connected) return true;
    
    peer.socket = socket(AF_INET, SOCK_STREAM, 0);
    if (peer.socket == INVALID_SOCK) return false;
    
    // Set non-blocking connect timeout
    struct timeval tv;
    tv.tv_sec = 1;
    tv.tv_usec = 0;
#ifdef _WIN32
    setsockopt(peer.socket, SOL_SOCKET, SO_RCVTIMEO, (const char*)&tv, sizeof(tv));
    setsockopt(peer.socket, SOL_SOCKET, SO_SNDTIMEO, (const char*)&tv, sizeof(tv));
#else
    setsockopt(peer.socket, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(peer.socket, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
#endif
    
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(peer.port);
    inet_pton(AF_INET, peer.host.c_str(), &addr.sin_addr);
    
    if (connect(peer.socket, (sockaddr*)&addr, sizeof(addr)) < 0) {
        CLOSE_SOCKET(peer.socket);
        peer.socket = INVALID_SOCK;
        return false;
    }
    
    peer.connected = true;
    std::cout << "[RAFT] Connected to peer " << peer.id << std::endl;
    return true;
}

void RaftNode::disconnectPeer(PeerInfo& peer) {
    if (peer.socket != INVALID_SOCK) {
        CLOSE_SOCKET(peer.socket);
        peer.socket = INVALID_SOCK;
    }
    peer.connected = false;
}

bool RaftNode::sendRawMessage(SocketType sock, const std::vector<uint8_t>& data) {
    uint32_t len = static_cast<uint32_t>(data.size());
    
    if (send(sock, reinterpret_cast<const char*>(&len), 4, 0) != 4) {
        return false;
    }
    
    size_t sent = 0;
    while (sent < data.size()) {
        int n = send(sock, reinterpret_cast<const char*>(data.data() + sent),
                     static_cast<int>(data.size() - sent), 0);
        if (n <= 0) return false;
        sent += n;
    }
    
    return true;
}

std::vector<uint8_t> RaftNode::recvRawMessage(SocketType sock) {
    uint32_t len = 0;
    int n = recv(sock, reinterpret_cast<char*>(&len), 4, 0);
    if (n != 4 || len == 0 || len > 10 * 1024 * 1024) {
        return {};
    }
    
    std::vector<uint8_t> data(len);
    size_t received = 0;
    while (received < len) {
        n = recv(sock, reinterpret_cast<char*>(data.data() + received),
                 static_cast<int>(len - received), 0);
        if (n <= 0) return {};
        received += n;
    }
    
    return data;
}

} // namespace dkv
