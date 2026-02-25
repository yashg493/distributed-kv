#include <iostream>
#include <sstream>
#include "network/client.hpp"

void printHelp() {
    std::cout << "Commands:\n";
    std::cout << "  put <key> <value>  - Store a key-value pair\n";
    std::cout << "  get <key>          - Retrieve a value\n";
    std::cout << "  del <key>          - Delete a key\n";
    std::cout << "  ping               - Check server connection\n";
    std::cout << "  quit               - Exit client\n";
}

int main(int argc, char* argv[]) {
    std::string host = "127.0.0.1";
    uint16_t port = 7878;

    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "-h" && i + 1 < argc) {
            host = argv[++i];
        } else if (arg == "-p" && i + 1 < argc) {
            port = static_cast<uint16_t>(std::stoi(argv[++i]));
        } else if (arg == "--help") {
            std::cout << "Usage: kv_client [-h host] [-p port]\n";
            std::cout << "  -h host     Server host (default: 127.0.0.1)\n";
            std::cout << "  -p port     Server port (default: 7878)\n";
            return 0;
        }
    }

    dkv::Client client;
    
    std::cout << "Connecting to " << host << ":" << port << "...\n";
    if (!client.connect(host, port)) {
        std::cerr << "Failed to connect to server\n";
        return 1;
    }
    std::cout << "Connected!\n\n";
    printHelp();
    std::cout << "\n";

    std::string line;
    while (true) {
        std::cout << "dkv> ";
        if (!std::getline(std::cin, line)) {
            break;
        }

        std::istringstream iss(line);
        std::string cmd;
        iss >> cmd;

        if (cmd.empty()) {
            continue;
        }

        try {
            if (cmd == "put") {
                std::string key, value;
                iss >> key;
                std::getline(iss >> std::ws, value);
                
                if (key.empty() || value.empty()) {
                    std::cout << "Usage: put <key> <value>\n";
                    continue;
                }
                
                if (client.put(key, value)) {
                    std::cout << "OK\n";
                } else {
                    std::cout << "ERROR\n";
                }
            }
            else if (cmd == "get") {
                std::string key;
                iss >> key;
                
                if (key.empty()) {
                    std::cout << "Usage: get <key>\n";
                    continue;
                }
                
                auto value = client.get(key);
                if (value) {
                    std::cout << *value << "\n";
                } else {
                    std::cout << "(nil)\n";
                }
            }
            else if (cmd == "del") {
                std::string key;
                iss >> key;
                
                if (key.empty()) {
                    std::cout << "Usage: del <key>\n";
                    continue;
                }
                
                if (client.del(key)) {
                    std::cout << "OK\n";
                } else {
                    std::cout << "ERROR\n";
                }
            }
            else if (cmd == "ping") {
                if (client.ping()) {
                    std::cout << "PONG\n";
                } else {
                    std::cout << "ERROR\n";
                }
            }
            else if (cmd == "quit" || cmd == "exit") {
                break;
            }
            else if (cmd == "help") {
                printHelp();
            }
            else {
                std::cout << "Unknown command. Type 'help' for usage.\n";
            }
        } catch (const std::exception& e) {
            std::cerr << "Error: " << e.what() << "\n";
            if (!client.isConnected()) {
                std::cerr << "Connection lost. Exiting.\n";
                break;
            }
        }
    }

    std::cout << "Bye!\n";
    return 0;
}
