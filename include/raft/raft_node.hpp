#pragma once

#include <string>
#include <vector>
#include <map>
#include <memory>
#include <thread>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <functional>
#include "storage/lsm_tree.hpp"
#include "network/protocol.hpp"
#include "raft/raft_state.hpp"

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

// Peer connection info
struct PeerInfo {
    std::string id;        // peer_host:peer_port
    std::string host;
    uint16_t port;
    SocketType socket = INVALID_SOCK;
    bool connected = false;
};

/**
 * RaftNode - A distributed KV store node using Raft consensus.
 * 
 * Handles:
 * - Leader election via RequestVote RPCs
 * - Log replication via AppendEntries RPCs
 * - Client requests (PUT, GET, DELETE)
 */
class RaftNode {
public:
    RaftNode(const std::string& data_dir, uint16_t port, 
             const std::vector<std::string>& peers);
    ~RaftNode();

    RaftNode(const RaftNode&) = delete;
    RaftNode& operator=(const RaftNode&) = delete;

    void start();
    void stop();
    bool isRunning() const { return running_; }
    
    // Status
    RaftRole getRole() const { return state_.getRole(); }
    uint64_t getCurrentTerm() const { return state_.getCurrentTerm(); }
    std::string getLeaderId() const { return state_.getLeaderId(); }
    std::string getNodeId() const { return state_.getNodeId(); }

private:
    // Main loops
    void acceptLoop();           // Accept client connections
    void raftLoop();             // Election timeout & heartbeat logic
    void peerConnectionLoop();   // Maintain peer connections
    
    // Client handling
    void handleClient(SocketType client_sock);
    Response processClientRequest(const Request& req);
    
    // Raft RPCs
    void startElection();
    void requestVoteFromPeer(PeerInfo& peer);
    RequestVoteResponse handleRequestVote(const RequestVote& rv);
    
    void sendHeartbeats();
    void sendAppendEntriesToPeer(PeerInfo& peer);
    AppendEntriesResponse handleAppendEntries(const AppendEntries& ae);
    
    // Log management
    uint64_t appendLog(OpCode op, const std::string& key, const std::string& value);
    RaftLogEntry getLogEntry(uint64_t index) const;
    uint64_t getLastLogIndex() const;
    uint64_t getLastLogTerm() const;
    void applyCommittedEntries();
    
    // State transitions
    void becomeFollower(uint64_t term);
    void becomeCandidate();
    void becomeLeader();
    
    // Network helpers
    bool connectToPeer(PeerInfo& peer);
    void disconnectPeer(PeerInfo& peer);
    bool sendMessage(SocketType sock, OpCode op, const std::vector<uint8_t>& data);
    std::pair<OpCode, std::vector<uint8_t>> recvMessage(SocketType sock);
    bool sendRawMessage(SocketType sock, const std::vector<uint8_t>& data);
    std::vector<uint8_t> recvRawMessage(SocketType sock);
    
    // Status response for clients
    Response buildStatusResponse() const;

    // Configuration
    uint16_t port_;
    std::string data_dir_;
    std::vector<PeerInfo> peers_;
    
    // State
    RaftState state_;
    std::atomic<bool> running_{false};
    
    // Storage
    std::unique_ptr<LSMTree> store_;
    std::vector<RaftLogEntry> log_;  // Raft log (index 0 is dummy)
    mutable std::mutex log_mutex_;
    
    // Election state
    std::atomic<int> votes_received_{0};
    std::mutex election_mutex_;
    
    // Server socket
    SocketType server_sock_ = INVALID_SOCK;
    
    // Threads
    std::thread accept_thread_;
    std::thread raft_thread_;
    std::thread peer_thread_;
    std::vector<std::thread> client_threads_;
    std::mutex threads_mutex_;
    std::mutex peers_mutex_;
    
    // Condition variable for raft loop
    std::condition_variable raft_cv_;
    std::mutex raft_mutex_;
};

} // namespace dkv
