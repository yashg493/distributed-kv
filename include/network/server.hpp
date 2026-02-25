#pragma once

#include <string>
#include <memory>
#include <thread>
#include <atomic>
#include <vector>
#include "storage/lsm_tree.hpp"
#include "network/protocol.hpp"

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
#include <unistd.h>
using SocketType = int;
#define INVALID_SOCK -1
#define CLOSE_SOCKET close
#endif

namespace dkv {

class Server {
public:
    Server(const std::string& data_dir, uint16_t port);
    ~Server();

    Server(const Server&) = delete;
    Server& operator=(const Server&) = delete;

    void start();
    void stop();
    bool isRunning() const { return running_; }

private:
    void acceptLoop();
    void handleClient(SocketType client_sock);
    Response processRequest(const Request& req);
    
    bool sendMessage(SocketType sock, const std::vector<uint8_t>& data);
    std::vector<uint8_t> recvMessage(SocketType sock);

    uint16_t port_;
    SocketType server_sock_ = INVALID_SOCK;
    std::atomic<bool> running_{false};
    std::unique_ptr<LSMTree> store_;
    std::thread accept_thread_;
    std::vector<std::thread> client_threads_;
    std::mutex threads_mutex_;
};

} // namespace dkv
