#include <iostream>
#include <csignal>
#include "network/server.hpp"

dkv::Server* g_server = nullptr;

void signalHandler(int signal) {
    if (g_server) {
        std::cout << "\nShutting down..." << std::endl;
        g_server->stop();
    }
}

int main(int argc, char* argv[]) {
    uint16_t port = 7878;
    std::string data_dir = "./server_data";

    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "-p" && i + 1 < argc) {
            port = static_cast<uint16_t>(std::stoi(argv[++i]));
        } else if (arg == "-d" && i + 1 < argc) {
            data_dir = argv[++i];
        } else if (arg == "-h" || arg == "--help") {
            std::cout << "Usage: kv_server [-p port] [-d data_dir]\n";
            std::cout << "  -p port     Port to listen on (default: 7878)\n";
            std::cout << "  -d dir      Data directory (default: ./server_data)\n";
            return 0;
        }
    }

    try {
        dkv::Server server(data_dir, port);
        g_server = &server;

        std::signal(SIGINT, signalHandler);
        std::signal(SIGTERM, signalHandler);

        server.start();

        std::cout << "Press Ctrl+C to stop the server\n";
        
        while (server.isRunning()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
