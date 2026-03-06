#pragma once

#include <string>
#include <memory>
#include <thread>
#include <atomic>
#include <vector>
#include <map>
#include <mutex>
#include <condition_variable>
#include "storage/lsm_tree.hpp"
#include "network/protocol.hpp"
#include "replication/replication_log.hpp"

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#pragma comment(lib, "ws2_32.lib")
using SocketType = SOCKET;
#define INVALID_SOCK INVALID_SOCKET
#define CLOSE_SOCKET closesocket
#else
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
using SocketType = int;
#define INVALID_SOCK -1
#define CLOSE_SOCKET close
#endif

namespace dkv {

// Information about a connected follower
struct FollowerInfo {
    std::string node_id;
    SocketType socket;
    uint64_t last_acked_seq;
    std::chrono::steady_clock::time_point last_heartbeat;
    bool connected;
};

/**
 * ReplicaNode - A server node that can operate as leader, follower, or standalone.
 * 
 * Leader: Accepts client writes, replicates to followers
 * Follower: Connects to leader, receives replicated writes
 * Standalone: Original single-node behavior
 */
class ReplicaNode {
public:
    ReplicaNode(const std::string& data_dir, uint16_t port, NodeRole role);
    ~ReplicaNode();

    ReplicaNode(const ReplicaNode&) = delete;
    ReplicaNode& operator=(const ReplicaNode&) = delete;

    // Configure as follower connecting to leader
    void setLeaderAddress(const std::string& host, uint16_t port);
    
    // Start the node
    void start();
    void stop();
    bool isRunning() const { return running_; }
    
    // Get node status
    NodeRole getRole() const { return role_; }
    uint64_t getLastSequence() const;
    size_t getFollowerCount() const;
    std::vector<std::string> getFollowerIds() const;

private:
    // Server functionality (accepts client and follower connections)
    void acceptLoop();
    void handleClient(SocketType client_sock);
    Response processRequest(const Request& req, bool from_replication = false);
    
    // Leader functionality
    void replicateToFollowers(const ReplicationEntry& entry);
    void handleFollowerJoin(SocketType sock, const Request& req);
    void heartbeatLoop();
    void sendToFollower(FollowerInfo& follower, const std::vector<uint8_t>& data);
    
    // Follower functionality
    void followerLoop();
    void connectToLeader();
    void handleReplication(const ReplicationEntry& entry);
    void sendJoinRequest();
    
    // Network helpers
    bool sendMessage(SocketType sock, const std::vector<uint8_t>& data);
    std::vector<uint8_t> recvMessage(SocketType sock);
    
    // Status response
    Response buildStatusResponse() const;

    // Configuration
    uint16_t port_;
    std::string data_dir_;
    NodeRole role_;
    std::string leader_host_;
    uint16_t leader_port_ = 0;
    
    // State
    SocketType server_sock_ = INVALID_SOCK;
    SocketType leader_sock_ = INVALID_SOCK;  // Follower's connection to leader
    std::atomic<bool> running_{false};
    
    // Storage
    std::unique_ptr<LSMTree> store_;
    std::unique_ptr<ReplicationLog> repl_log_;
    
    // Leader state
    std::map<std::string, FollowerInfo> followers_;
    std::mutex followers_mutex_;
    
    // Follower state
    uint64_t last_applied_seq_ = 0;
    std::mutex follower_mutex_;
    
    // Threads
    std::thread accept_thread_;
    std::thread follower_thread_;
    std::thread heartbeat_thread_;
    std::vector<std::thread> client_threads_;
    std::mutex threads_mutex_;
};

} // namespace dkv
