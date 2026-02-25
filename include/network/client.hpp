#pragma once

#include <string>
#include <optional>
#include <vector>
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
#include <arpa/inet.h>
#include <unistd.h>
using SocketType = int;
#define INVALID_SOCK -1
#define CLOSE_SOCKET close
#endif

namespace dkv {

class Client {
public:
    Client();
    ~Client();

    Client(const Client&) = delete;
    Client& operator=(const Client&) = delete;

    bool connect(const std::string& host, uint16_t port);
    void disconnect();
    bool isConnected() const { return connected_; }

    bool put(const std::string& key, const std::string& value);
    std::optional<std::string> get(const std::string& key);
    bool del(const std::string& key);
    bool ping();

private:
    Response sendRequest(const Request& req);
    bool sendMessage(const std::vector<uint8_t>& data);
    std::vector<uint8_t> recvMessage();

    SocketType sock_ = INVALID_SOCK;
    bool connected_ = false;
};

} // namespace dkv
