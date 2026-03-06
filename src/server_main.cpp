#include <iostream>
#include <csignal>
#include "replication/replica_node.hpp"

dkv::ReplicaNode* g_node = nullptr;

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
    std::cout << "  --role ROLE   Node role: standalone, leader, follower (default: standalone)\n";
    std::cout << "  --leader H:P  Leader address for follower mode (e.g., 127.0.0.1:7878)\n";
    std::cout << "  -h, --help    Show this help\n";
    std::cout << "\nExamples:\n";
    std::cout << "  kv_server -p 7878 --role leader\n";
    std::cout << "  kv_server -p 7879 --role follower --leader 127.0.0.1:7878\n";
}

int main(int argc, char* argv[]) {
    uint16_t port = 7878;
    std::string data_dir = "./server_data";
    dkv::NodeRole role = dkv::NodeRole::STANDALONE;
    std::string leader_host;
    uint16_t leader_port = 0;

    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "-p" && i + 1 < argc) {
            port = static_cast<uint16_t>(std::stoi(argv[++i]));
        } else if (arg == "-d" && i + 1 < argc) {
            data_dir = argv[++i];
        } else if (arg == "--role" && i + 1 < argc) {
            std::string role_str = argv[++i];
            if (role_str == "leader") {
                role = dkv::NodeRole::LEADER;
            } else if (role_str == "follower") {
                role = dkv::NodeRole::FOLLOWER;
            } else if (role_str == "standalone") {
                role = dkv::NodeRole::STANDALONE;
            } else {
                std::cerr << "Unknown role: " << role_str << "\n";
                return 1;
            }
        } else if (arg == "--leader" && i + 1 < argc) {
            std::string addr = argv[++i];
            auto colon = addr.find(':');
            if (colon == std::string::npos) {
                std::cerr << "Invalid leader address format. Use host:port\n";
                return 1;
            }
            leader_host = addr.substr(0, colon);
            leader_port = static_cast<uint16_t>(std::stoi(addr.substr(colon + 1)));
        } else if (arg == "-h" || arg == "--help") {
            printUsage();
            return 0;
        }
    }

    // Validate follower configuration
    if (role == dkv::NodeRole::FOLLOWER && (leader_host.empty() || leader_port == 0)) {
        std::cerr << "Error: Follower mode requires --leader option\n";
        return 1;
    }

    try {
        dkv::ReplicaNode node(data_dir, port, role);
        g_node = &node;

        if (role == dkv::NodeRole::FOLLOWER) {
            node.setLeaderAddress(leader_host, leader_port);
        }

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
