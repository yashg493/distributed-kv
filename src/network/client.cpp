#include "network/client.hpp"
#include <stdexcept>
#include <cstring>

namespace dkv {

Client::Client() {
#ifdef _WIN32
    WSADATA wsa;
    WSAStartup(MAKEWORD(2, 2), &wsa);
#endif
}

Client::~Client() {
    disconnect();
#ifdef _WIN32
    WSACleanup();
#endif
}

bool Client::connect(const std::string& host, uint16_t port) {
    if (connected_) {
        disconnect();
    }

    sock_ = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (sock_ == INVALID_SOCK) {
        return false;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    
    if (inet_pton(AF_INET, host.c_str(), &addr.sin_addr) <= 0) {
        CLOSE_SOCKET(sock_);
        sock_ = INVALID_SOCK;
        return false;
    }

    if (::connect(sock_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        CLOSE_SOCKET(sock_);
        sock_ = INVALID_SOCK;
        return false;
    }

    connected_ = true;
    return true;
}

void Client::disconnect() {
    if (sock_ != INVALID_SOCK) {
        CLOSE_SOCKET(sock_);
        sock_ = INVALID_SOCK;
    }
    connected_ = false;
}

bool Client::put(const std::string& key, const std::string& value) {
    Request req{OpCode::OP_PUT, key, value};
    Response resp = sendRequest(req);
    return resp.status == StatusCode::STATUS_OK;
}

std::optional<std::string> Client::get(const std::string& key) {
    Request req{OpCode::OP_GET, key, ""};
    Response resp = sendRequest(req);
    
    if (resp.status == StatusCode::STATUS_OK) {
        return resp.value;
    }
    return std::nullopt;
}

bool Client::del(const std::string& key) {
    Request req{OpCode::OP_DELETE, key, ""};
    Response resp = sendRequest(req);
    return resp.status == StatusCode::STATUS_OK;
}

bool Client::ping() {
    Request req{OpCode::OP_PING, "", ""};
    Response resp = sendRequest(req);
    return resp.status == StatusCode::STATUS_OK && resp.value == "PONG";
}

Response Client::sendRequest(const Request& req) {
    if (!connected_) {
        throw std::runtime_error("Not connected");
    }

    auto data = req.serialize();
    if (!sendMessage(data)) {
        connected_ = false;
        throw std::runtime_error("Failed to send request");
    }

    auto resp_data = recvMessage();
    if (resp_data.empty()) {
        connected_ = false;
        throw std::runtime_error("Failed to receive response");
    }

    return Response::deserialize(resp_data);
}

bool Client::sendMessage(const std::vector<uint8_t>& data) {
    uint32_t len = static_cast<uint32_t>(data.size());
    uint8_t header[4] = {
        static_cast<uint8_t>((len >> 0) & 0xFF),
        static_cast<uint8_t>((len >> 8) & 0xFF),
        static_cast<uint8_t>((len >> 16) & 0xFF),
        static_cast<uint8_t>((len >> 24) & 0xFF)
    };

    if (send(sock_, reinterpret_cast<const char*>(header), 4, 0) != 4) {
        return false;
    }

    size_t sent = 0;
    while (sent < data.size()) {
        int n = send(sock_, reinterpret_cast<const char*>(data.data() + sent),
                     static_cast<int>(data.size() - sent), 0);
        if (n <= 0) return false;
        sent += n;
    }

    return true;
}

std::vector<uint8_t> Client::recvMessage() {
    uint8_t header[4];
    int received = 0;
    while (received < 4) {
        int n = recv(sock_, reinterpret_cast<char*>(header + received), 4 - received, 0);
        if (n <= 0) return {};
        received += n;
    }

    uint32_t len = header[0] | (header[1] << 8) | (header[2] << 16) | (header[3] << 24);
    if (len > 10 * 1024 * 1024) {
        return {};
    }

    std::vector<uint8_t> data(len);
    received = 0;
    while (received < static_cast<int>(len)) {
        int n = recv(sock_, reinterpret_cast<char*>(data.data() + received),
                     static_cast<int>(len - received), 0);
        if (n <= 0) return {};
        received += n;
    }

    return data;
}

} // namespace dkv
