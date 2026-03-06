#include "replication/replica_node.hpp"
#include <iostream>
#include <cstring>
#include <sstream>
#include <random>

namespace dkv {

// Generate a random node ID
static std::string generateNodeId() {
    static std::random_device rd;
    static std::mt19937 gen(rd());
    static std::uniform_int_distribution<> dis(0, 15);
    
    std::stringstream ss;
    ss << "node-";
    for (int i = 0; i < 8; ++i) {
        ss << std::hex << dis(gen);
    }
    return ss.str();
}

ReplicaNode::ReplicaNode(const std::string& data_dir, uint16_t port, NodeRole role)
    : port_(port), data_dir_(data_dir), role_(role) {
    
#ifdef _WIN32
    WSADATA wsaData;
    WSAStartup(MAKEWORD(2, 2), &wsaData);
#endif
    
    // Initialize storage
    store_ = std::make_unique<LSMTree>(data_dir);
    
    // Initialize replication log (only for leader/standalone)
    if (role_ == NodeRole::LEADER || role_ == NodeRole::STANDALONE) {
        std::string log_path = data_dir + "/replication.log";
        repl_log_ = std::make_unique<ReplicationLog>(log_path);
    }
}

ReplicaNode::~ReplicaNode() {
    stop();
    
#ifdef _WIN32
    WSACleanup();
#endif
}

void ReplicaNode::setLeaderAddress(const std::string& host, uint16_t port) {
    leader_host_ = host;
    leader_port_ = port;
}

void ReplicaNode::start() {
    if (running_) return;
    running_ = true;
    
    // Create server socket
    server_sock_ = socket(AF_INET, SOCK_STREAM, 0);
    if (server_sock_ == INVALID_SOCK) {
        throw std::runtime_error("Failed to create socket");
    }
    
    // Allow port reuse
    int opt = 1;
#ifdef _WIN32
    setsockopt(server_sock_, SOL_SOCKET, SO_REUSEADDR, (const char*)&opt, sizeof(opt));
#else
    setsockopt(server_sock_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
#endif
    
    // Bind
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port_);
    
    if (bind(server_sock_, (sockaddr*)&addr, sizeof(addr)) < 0) {
        CLOSE_SOCKET(server_sock_);
        throw std::runtime_error("Failed to bind to port " + std::to_string(port_));
    }
    
    // Listen
    if (listen(server_sock_, 10) < 0) {
        CLOSE_SOCKET(server_sock_);
        throw std::runtime_error("Failed to listen");
    }
    
    std::string role_str = (role_ == NodeRole::LEADER) ? "LEADER" : 
                           (role_ == NodeRole::FOLLOWER) ? "FOLLOWER" : "STANDALONE";
    std::cout << "[" << role_str << "] Listening on port " << port_ << std::endl;
    
    // Start accept thread
    accept_thread_ = std::thread(&ReplicaNode::acceptLoop, this);
    
    // Start role-specific threads
    if (role_ == NodeRole::FOLLOWER) {
        follower_thread_ = std::thread(&ReplicaNode::followerLoop, this);
    } else if (role_ == NodeRole::LEADER) {
        heartbeat_thread_ = std::thread(&ReplicaNode::heartbeatLoop, this);
    }
}

void ReplicaNode::stop() {
    if (!running_) return;
    running_ = false;
    
    // Close server socket to unblock accept
    if (server_sock_ != INVALID_SOCK) {
        CLOSE_SOCKET(server_sock_);
        server_sock_ = INVALID_SOCK;
    }
    
    // Close leader connection (if follower)
    if (leader_sock_ != INVALID_SOCK) {
        CLOSE_SOCKET(leader_sock_);
        leader_sock_ = INVALID_SOCK;
    }
    
    // Close follower connections (if leader)
    {
        std::lock_guard<std::mutex> lock(followers_mutex_);
        for (auto& [id, follower] : followers_) {
            if (follower.socket != INVALID_SOCK) {
                CLOSE_SOCKET(follower.socket);
            }
        }
        followers_.clear();
    }
    
    // Join threads
    if (accept_thread_.joinable()) accept_thread_.join();
    if (follower_thread_.joinable()) follower_thread_.join();
    if (heartbeat_thread_.joinable()) heartbeat_thread_.join();
    
    {
        std::lock_guard<std::mutex> lock(threads_mutex_);
        for (auto& t : client_threads_) {
            if (t.joinable()) t.join();
        }
        client_threads_.clear();
    }
}

uint64_t ReplicaNode::getLastSequence() const {
    if (repl_log_) {
        return repl_log_->getLastSequence();
    }
    return last_applied_seq_;
}

size_t ReplicaNode::getFollowerCount() const {
    std::lock_guard<std::mutex> lock(const_cast<std::mutex&>(followers_mutex_));
    return followers_.size();
}

std::vector<std::string> ReplicaNode::getFollowerIds() const {
    std::lock_guard<std::mutex> lock(const_cast<std::mutex&>(followers_mutex_));
    std::vector<std::string> ids;
    for (const auto& [id, _] : followers_) {
        ids.push_back(id);
    }
    return ids;
}

// ==================== Server Functionality ====================

void ReplicaNode::acceptLoop() {
    while (running_) {
        sockaddr_in client_addr{};
        socklen_t addr_len = sizeof(client_addr);
        
        SocketType client_sock = accept(server_sock_, (sockaddr*)&client_addr, &addr_len);
        if (client_sock == INVALID_SOCK) {
            if (running_) {
                std::cerr << "Accept failed" << std::endl;
            }
            continue;
        }
        
        // Spawn thread to handle client
        std::lock_guard<std::mutex> lock(threads_mutex_);
        client_threads_.emplace_back(&ReplicaNode::handleClient, this, client_sock);
    }
}

void ReplicaNode::handleClient(SocketType client_sock) {
    while (running_) {
        std::vector<uint8_t> data = recvMessage(client_sock);
        if (data.empty()) break;
        
        try {
            Request req = Request::deserialize(data);
            
            // Handle special replication requests
            if (req.op == OpCode::OP_JOIN_CLUSTER && role_ == NodeRole::LEADER) {
                handleFollowerJoin(client_sock, req);
                continue;
            }
            
            Response resp = processRequest(req);
            std::vector<uint8_t> resp_data = resp.serialize();
            
            if (!sendMessage(client_sock, resp_data)) {
                break;
            }
        } catch (const std::exception& e) {
            Response resp{StatusCode::STATUS_ERROR, "", e.what()};
            sendMessage(client_sock, resp.serialize());
        }
    }
    
    CLOSE_SOCKET(client_sock);
}

Response ReplicaNode::processRequest(const Request& req, bool from_replication) {
    Response resp;
    resp.status = StatusCode::STATUS_OK;
    
    switch (req.op) {
        case OpCode::OP_PUT: {
            // If leader, replicate first
            if (role_ == NodeRole::LEADER && !from_replication && repl_log_) {
                uint64_t seq = repl_log_->append(OpCode::OP_PUT, req.key, req.value);
                
                // Build replication entry
                ReplicationEntry entry;
                entry.sequence_num = seq;
                entry.op = OpCode::OP_PUT;
                entry.key = req.key;
                entry.value = req.value;
                entry.timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch()
                ).count();
                
                replicateToFollowers(entry);
            }
            
            store_->put(req.key, req.value);
            break;
        }
        
        case OpCode::OP_GET: {
            auto value = store_->get(req.key);
            if (value) {
                resp.value = *value;
            } else {
                resp.status = StatusCode::STATUS_NOT_FOUND;
            }
            break;
        }
        
        case OpCode::OP_DELETE: {
            // If leader, replicate first
            if (role_ == NodeRole::LEADER && !from_replication && repl_log_) {
                uint64_t seq = repl_log_->append(OpCode::OP_DELETE, req.key, "");
                
                ReplicationEntry entry;
                entry.sequence_num = seq;
                entry.op = OpCode::OP_DELETE;
                entry.key = req.key;
                entry.value = "";
                entry.timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch()
                ).count();
                
                replicateToFollowers(entry);
            }
            
            store_->del(req.key);
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

Response ReplicaNode::buildStatusResponse() const {
    Response resp;
    resp.status = StatusCode::STATUS_OK;
    
    std::stringstream ss;
    ss << "role:";
    switch (role_) {
        case NodeRole::LEADER: ss << "leader"; break;
        case NodeRole::FOLLOWER: ss << "follower"; break;
        case NodeRole::STANDALONE: ss << "standalone"; break;
    }
    ss << "\n";
    ss << "sequence:" << getLastSequence() << "\n";
    
    if (role_ == NodeRole::LEADER) {
        ss << "followers:" << getFollowerCount() << "\n";
        auto ids = getFollowerIds();
        for (const auto& id : ids) {
            ss << "  - " << id << "\n";
        }
    } else if (role_ == NodeRole::FOLLOWER) {
        ss << "leader:" << leader_host_ << ":" << leader_port_ << "\n";
        ss << "connected:" << (leader_sock_ != INVALID_SOCK ? "yes" : "no") << "\n";
    }
    
    resp.value = ss.str();
    return resp;
}

// ==================== Leader Functionality ====================

void ReplicaNode::replicateToFollowers(const ReplicationEntry& entry) {
    std::lock_guard<std::mutex> lock(followers_mutex_);
    
    // Build replication request
    Request req;
    req.op = OpCode::OP_REPLICATE;
    req.key = "";  // Not used
    req.value = "";  // Entry is in the serialized form
    
    // Serialize the entry as the request value
    auto entry_data = entry.serialize();
    req.value = std::string(entry_data.begin(), entry_data.end());
    
    auto req_data = req.serialize();
    
    for (auto& [id, follower] : followers_) {
        if (follower.connected) {
            sendToFollower(follower, req_data);
        }
    }
}

void ReplicaNode::handleFollowerJoin(SocketType sock, const Request& req) {
    std::string node_id = req.key.empty() ? generateNodeId() : req.key;
    
    std::cout << "[LEADER] Follower joining: " << node_id << std::endl;
    
    // Parse requested sequence number from value
    uint64_t requested_seq = 0;
    if (!req.value.empty()) {
        requested_seq = std::stoull(req.value);
    }
    
    // Send acknowledgment
    Response resp;
    resp.status = StatusCode::STATUS_OK;
    resp.value = node_id;
    sendMessage(sock, resp.serialize());
    
    // Add to followers
    {
        std::lock_guard<std::mutex> lock(followers_mutex_);
        FollowerInfo info;
        info.node_id = node_id;
        info.socket = sock;
        info.last_acked_seq = requested_seq;
        info.last_heartbeat = std::chrono::steady_clock::now();
        info.connected = true;
        followers_[node_id] = info;
    }
    
    // Send missed entries
    if (repl_log_) {
        auto entries = repl_log_->getEntriesSince(requested_seq);
        for (const auto& entry : entries) {
            Request repl_req;
            repl_req.op = OpCode::OP_REPLICATE;
            auto entry_data = entry.serialize();
            repl_req.value = std::string(entry_data.begin(), entry_data.end());
            
            std::lock_guard<std::mutex> lock(followers_mutex_);
            auto it = followers_.find(node_id);
            if (it != followers_.end() && it->second.connected) {
                sendToFollower(it->second, repl_req.serialize());
            }
        }
    }
    
    // Keep connection open for ongoing replication (handled in separate messages)
    // The socket stays open in the followers_ map
}

void ReplicaNode::heartbeatLoop() {
    while (running_) {
        std::this_thread::sleep_for(std::chrono::seconds(5));
        
        std::lock_guard<std::mutex> lock(followers_mutex_);
        
        Request req;
        req.op = OpCode::OP_HEARTBEAT;
        auto data = req.serialize();
        
        for (auto& [id, follower] : followers_) {
            if (follower.connected) {
                if (!sendMessage(follower.socket, data)) {
                    std::cout << "[LEADER] Follower disconnected: " << id << std::endl;
                    follower.connected = false;
                }
            }
        }
    }
}

void ReplicaNode::sendToFollower(FollowerInfo& follower, const std::vector<uint8_t>& data) {
    if (!sendMessage(follower.socket, data)) {
        follower.connected = false;
        std::cout << "[LEADER] Failed to send to follower: " << follower.node_id << std::endl;
    }
}

// ==================== Follower Functionality ====================

void ReplicaNode::followerLoop() {
    while (running_) {
        if (leader_sock_ == INVALID_SOCK) {
            connectToLeader();
            if (leader_sock_ == INVALID_SOCK) {
                std::this_thread::sleep_for(std::chrono::seconds(2));
                continue;
            }
            sendJoinRequest();
        }
        
        // Receive messages from leader
        std::vector<uint8_t> data = recvMessage(leader_sock_);
        if (data.empty()) {
            std::cout << "[FOLLOWER] Lost connection to leader" << std::endl;
            CLOSE_SOCKET(leader_sock_);
            leader_sock_ = INVALID_SOCK;
            continue;
        }
        
        try {
            Request req = Request::deserialize(data);
            
            if (req.op == OpCode::OP_REPLICATE) {
                // Deserialize replication entry from req.value
                std::vector<uint8_t> entry_data(req.value.begin(), req.value.end());
                ReplicationEntry entry = ReplicationEntry::deserialize(entry_data);
                handleReplication(entry);
                
                // Send ack
                Request ack;
                ack.op = OpCode::OP_REPLICATE_ACK;
                ack.value = std::to_string(entry.sequence_num);
                sendMessage(leader_sock_, ack.serialize());
                
            } else if (req.op == OpCode::OP_HEARTBEAT) {
                // Respond to heartbeat
                Response resp{StatusCode::STATUS_OK, "OK", ""};
                sendMessage(leader_sock_, resp.serialize());
            }
        } catch (const std::exception& e) {
            std::cerr << "[FOLLOWER] Error processing message: " << e.what() << std::endl;
        }
    }
}

void ReplicaNode::connectToLeader() {
    if (leader_host_.empty() || leader_port_ == 0) {
        std::cerr << "[FOLLOWER] Leader address not configured" << std::endl;
        return;
    }
    
    leader_sock_ = socket(AF_INET, SOCK_STREAM, 0);
    if (leader_sock_ == INVALID_SOCK) {
        std::cerr << "[FOLLOWER] Failed to create socket" << std::endl;
        return;
    }
    
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(leader_port_);
    inet_pton(AF_INET, leader_host_.c_str(), &addr.sin_addr);
    
    if (connect(leader_sock_, (sockaddr*)&addr, sizeof(addr)) < 0) {
        std::cerr << "[FOLLOWER] Failed to connect to leader at " 
                  << leader_host_ << ":" << leader_port_ << std::endl;
        CLOSE_SOCKET(leader_sock_);
        leader_sock_ = INVALID_SOCK;
        return;
    }
    
    std::cout << "[FOLLOWER] Connected to leader at " 
              << leader_host_ << ":" << leader_port_ << std::endl;
}

void ReplicaNode::handleReplication(const ReplicationEntry& entry) {
    std::lock_guard<std::mutex> lock(follower_mutex_);
    
    // Skip if already applied
    if (entry.sequence_num <= last_applied_seq_) {
        return;
    }
    
    // Apply the operation
    switch (entry.op) {
        case OpCode::OP_PUT:
            store_->put(entry.key, entry.value);
            break;
        case OpCode::OP_DELETE:
            store_->del(entry.key);
            break;
        default:
            break;
    }
    
    last_applied_seq_ = entry.sequence_num;
    std::cout << "[FOLLOWER] Applied seq " << entry.sequence_num 
              << ": " << (entry.op == OpCode::OP_PUT ? "PUT" : "DEL") 
              << " " << entry.key << std::endl;
}

void ReplicaNode::sendJoinRequest() {
    Request req;
    req.op = OpCode::OP_JOIN_CLUSTER;
    req.key = generateNodeId();  // Our node ID
    req.value = std::to_string(last_applied_seq_);  // Request entries after this
    
    sendMessage(leader_sock_, req.serialize());
    
    // Wait for ack
    auto data = recvMessage(leader_sock_);
    if (!data.empty()) {
        Response resp = Response::deserialize(data);
        if (resp.status == StatusCode::STATUS_OK) {
            std::cout << "[FOLLOWER] Joined cluster as: " << resp.value << std::endl;
        }
    }
}

// ==================== Network Helpers ====================

bool ReplicaNode::sendMessage(SocketType sock, const std::vector<uint8_t>& data) {
    uint32_t len = static_cast<uint32_t>(data.size());
    
    // Send length
    if (send(sock, reinterpret_cast<const char*>(&len), 4, 0) != 4) {
        return false;
    }
    
    // Send data
    size_t sent = 0;
    while (sent < data.size()) {
        int n = send(sock, reinterpret_cast<const char*>(data.data() + sent), 
                     static_cast<int>(data.size() - sent), 0);
        if (n <= 0) return false;
        sent += n;
    }
    
    return true;
}

std::vector<uint8_t> ReplicaNode::recvMessage(SocketType sock) {
    // Receive length
    uint32_t len = 0;
    int n = recv(sock, reinterpret_cast<char*>(&len), 4, 0);
    if (n != 4 || len == 0 || len > 10 * 1024 * 1024) {
        return {};
    }
    
    // Receive data
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
