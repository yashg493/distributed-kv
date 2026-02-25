#include "network/server.hpp"
#include <iostream>
#include <cstring>

namespace dkv {

Server::Server(const std::string& data_dir, uint16_t port) : port_(port) {
#ifdef _WIN32
    WSADATA wsa;
    if (WSAStartup(MAKEWORD(2, 2), &wsa) != 0) {
        throw std::runtime_error("WSAStartup failed");
    }
#endif
    store_ = std::make_unique<LSMTree>(data_dir);
}

Server::~Server() {
    stop();
#ifdef _WIN32
    WSACleanup();
#endif
}

void Server::start() {
    server_sock_ = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (server_sock_ == INVALID_SOCK) {
        throw std::runtime_error("Failed to create socket");
    }

    int opt = 1;
    setsockopt(server_sock_, SOL_SOCKET, SO_REUSEADDR, 
               reinterpret_cast<const char*>(&opt), sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port_);

    if (bind(server_sock_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        CLOSE_SOCKET(server_sock_);
        throw std::runtime_error("Failed to bind to port " + std::to_string(port_));
    }

    if (listen(server_sock_, 10) < 0) {
        CLOSE_SOCKET(server_sock_);
        throw std::runtime_error("Failed to listen");
    }

    running_ = true;
    accept_thread_ = std::thread(&Server::acceptLoop, this);
    
    std::cout << "[Server] Listening on port " << port_ << std::endl;
}

void Server::stop() {
    if (!running_) return;
    
    running_ = false;
    
    if (server_sock_ != INVALID_SOCK) {
        CLOSE_SOCKET(server_sock_);
        server_sock_ = INVALID_SOCK;
    }
    
    if (accept_thread_.joinable()) {
        accept_thread_.join();
    }
    
    std::lock_guard<std::mutex> lock(threads_mutex_);
    for (auto& t : client_threads_) {
        if (t.joinable()) {
            t.join();
        }
    }
    client_threads_.clear();
    
    std::cout << "[Server] Stopped" << std::endl;
}

void Server::acceptLoop() {
    while (running_) {
        sockaddr_in client_addr{};
        socklen_t client_len = sizeof(client_addr);
        
        SocketType client_sock = accept(server_sock_, 
            reinterpret_cast<sockaddr*>(&client_addr), &client_len);
            
        if (client_sock == INVALID_SOCK) {
            if (running_) {
                std::cerr << "[Server] Accept failed" << std::endl;
            }
            continue;
        }
        
        std::lock_guard<std::mutex> lock(threads_mutex_);
        client_threads_.emplace_back(&Server::handleClient, this, client_sock);
    }
}

void Server::handleClient(SocketType client_sock) {
    while (running_) {
        auto data = recvMessage(client_sock);
        if (data.empty()) {
            break;
        }
        
        try {
            Request req = Request::deserialize(data);
            Response resp = processRequest(req);
            auto resp_data = resp.serialize();
            
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

Response Server::processRequest(const Request& req) {
    Response resp;
    
    switch (req.op) {
        case OpCode::OP_PUT:
            store_->put(req.key, req.value);
            resp.status = StatusCode::STATUS_OK;
            break;
            
        case OpCode::OP_GET: {
            auto value = store_->get(req.key);
            if (value) {
                resp.status = StatusCode::STATUS_OK;
                resp.value = *value;
            } else {
                resp.status = StatusCode::STATUS_NOT_FOUND;
            }
            break;
        }
        
        case OpCode::OP_DELETE:
            store_->del(req.key);
            resp.status = StatusCode::STATUS_OK;
            break;
            
        case OpCode::OP_PING:
            resp.status = StatusCode::STATUS_OK;
            resp.value = "PONG";
            break;
            
        default:
            resp.status = StatusCode::STATUS_ERROR;
            resp.error = "Unknown operation";
    }
    
    return resp;
}

bool Server::sendMessage(SocketType sock, const std::vector<uint8_t>& data) {
    uint32_t len = static_cast<uint32_t>(data.size());
    uint8_t header[4] = {
        static_cast<uint8_t>((len >> 0) & 0xFF),
        static_cast<uint8_t>((len >> 8) & 0xFF),
        static_cast<uint8_t>((len >> 16) & 0xFF),
        static_cast<uint8_t>((len >> 24) & 0xFF)
    };
    
    if (send(sock, reinterpret_cast<const char*>(header), 4, 0) != 4) {
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

std::vector<uint8_t> Server::recvMessage(SocketType sock) {
    uint8_t header[4];
    int received = 0;
    while (received < 4) {
        int n = recv(sock, reinterpret_cast<char*>(header + received), 4 - received, 0);
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
        int n = recv(sock, reinterpret_cast<char*>(data.data() + received), 
                     static_cast<int>(len - received), 0);
        if (n <= 0) return {};
        received += n;
    }
    
    return data;
}

} // namespace dkv
