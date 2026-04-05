#include <iostream>
#include <csignal>
#include <sstream>
#include <algorithm>
#include "raft/raft_node.hpp"

dkv::RaftNode* g_node = nullptr;

void signalHandler(int signal) {
    if (g_node) {
        std::cout << "\nShutting down..." << std::endl;
        g_node->stop();
    }
}

void printUsage() {
    std::cout << "Usage: kv_server [options]\n";
    std::cout << "Options:\n";
    std::cout << "  -p port       Port to listen on (default: 7878)\n";
    std::cout << "  -d dir        Data directory (default: ./server_data)\n";
    std::cout << "  --peers LIST  Comma-separated list of all cluster nodes (including self)\n";
    std::cout << "                e.g., 127.0.0.1:9000,127.0.0.1:9001,127.0.0.1:9002\n";
    std::cout << "  -h, --help    Show this help\n";
    std::cout << "\nExamples:\n";
    std::cout << "  # Start a 3-node Raft cluster:\n";
    std::cout << "  kv_server -p 9000 -d ./data0 --peers 127.0.0.1:9000,127.0.0.1:9001,127.0.0.1:9002\n";
    std::cout << "  kv_server -p 9001 -d ./data1 --peers 127.0.0.1:9000,127.0.0.1:9001,127.0.0.1:9002\n";
    std::cout << "  kv_server -p 9002 -d ./data2 --peers 127.0.0.1:9000,127.0.0.1:9001,127.0.0.1:9002\n";
}

std::vector<std::string> parsePeers(const std::string& peers_str) {
    std::vector<std::string> peers;
    std::stringstream ss(peers_str);
    std::string peer;
    while (std::getline(ss, peer, ',')) {
        if (!peer.empty()) {
            peers.push_back(peer);
        }
    }
    return peers;
}

int main(int argc, char* argv[]) {
    uint16_t port = 7878;
    std::string data_dir = "./server_data";
    std::vector<std::string> peers;

    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "-p" && i + 1 < argc) {
            port = static_cast<uint16_t>(std::stoi(argv[++i]));
        } else if (arg == "-d" && i + 1 < argc) {
            data_dir = argv[++i];
        } else if (arg == "--peers" && i + 1 < argc) {
            peers = parsePeers(argv[++i]);
        } else if (arg == "-h" || arg == "--help") {
            printUsage();
            return 0;
        }
    }

    // Add self to peers if not present
    std::string self = "127.0.0.1:" + std::to_string(port);
    if (std::find(peers.begin(), peers.end(), self) == peers.end()) {
        peers.push_back(self);
    }

    try {
        dkv::RaftNode node(data_dir, port, peers);
        g_node = &node;

        std::signal(SIGINT, signalHandler);
        std::signal(SIGTERM, signalHandler);

        node.start();

        std::cout << "Press Ctrl+C to stop the server\n";
        
        while (node.isRunning()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
