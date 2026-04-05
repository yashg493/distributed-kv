#include <iostream>
#include <sstream>
#include <algorithm>
#include "shard/sharded_client.hpp"

void printHelp() {
    std::cout << "Commands:\n";
    std::cout << "  put <key> <value>  - Store a key-value pair\n";
    std::cout << "  get <key>          - Retrieve a value\n";
    std::cout << "  del <key>          - Delete a key\n";
    std::cout << "  shard <key>        - Show which shard owns a key\n";
    std::cout << "  shards             - List all shards\n";
    std::cout << "  ping               - Ping all shards\n";
    std::cout << "  quit               - Exit client\n";
}

std::vector<std::string> parseShards(const std::string& shards_str) {
    std::vector<std::string> shards;
    std::stringstream ss(shards_str);
    std::string shard;
    while (std::getline(ss, shard, ',')) {
        if (!shard.empty()) {
            shards.push_back(shard);
        }
    }
    return shards;
}

int main(int argc, char* argv[]) {
    std::vector<std::string> shards;

    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "--shards" && i + 1 < argc) {
            shards = parseShards(argv[++i]);
        } else if (arg == "-h" || arg == "--help") {
            std::cout << "Usage: kv_sharded_client --shards host1:port1,host2:port2,...\n";
            std::cout << "  --shards LIST   Comma-separated list of shard addresses\n";
            std::cout << "\nExample:\n";
            std::cout << "  kv_sharded_client --shards 127.0.0.1:9000,127.0.0.1:9001,127.0.0.1:9002\n";
            return 0;
        }
    }

    if (shards.empty()) {
        std::cerr << "Error: No shards specified. Use --shards option.\n";
        std::cerr << "Example: kv_sharded_client --shards 127.0.0.1:9000,127.0.0.1:9001\n";
        return 1;
    }

    dkv::ShardedClient client;
    
    std::cout << "Connecting to " << shards.size() << " shards...\n";
    if (!client.initialize(shards)) {
        std::cerr << "Failed to connect to all shards\n";
        return 1;
    }
    std::cout << "Connected to " << client.shardCount() << " shards!\n\n";
    printHelp();
    std::cout << "\n";

    std::string line;
    while (true) {
        std::cout << "dkv-sharded> ";
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
                
                std::string shard = client.getShardForKey(key);
                if (client.put(key, value)) {
                    std::cout << "OK (shard: " << shard << ")\n";
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
                
                std::string shard = client.getShardForKey(key);
                auto value = client.get(key);
                if (value) {
                    std::cout << *value << " (shard: " << shard << ")\n";
                } else {
                    std::cout << "(nil) (shard: " << shard << ")\n";
                }
            }
            else if (cmd == "del") {
                std::string key;
                iss >> key;
                
                if (key.empty()) {
                    std::cout << "Usage: del <key>\n";
                    continue;
                }
                
                std::string shard = client.getShardForKey(key);
                if (client.del(key)) {
                    std::cout << "OK (shard: " << shard << ")\n";
                } else {
                    std::cout << "ERROR\n";
                }
            }
            else if (cmd == "shard") {
                std::string key;
                iss >> key;
                
                if (key.empty()) {
                    std::cout << "Usage: shard <key>\n";
                    continue;
                }
                
                std::string shard = client.getShardForKey(key);
                std::cout << shard << "\n";
            }
            else if (cmd == "shards") {
                auto shardList = client.getShards();
                std::cout << "Shards (" << shardList.size() << "):\n";
                for (const auto& s : shardList) {
                    std::cout << "  " << s << "\n";
                }
            }
            else if (cmd == "ping") {
                if (client.pingAll()) {
                    std::cout << "PONG (all " << client.shardCount() << " shards)\n";
                } else {
                    std::cout << "ERROR (some shards not responding)\n";
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
        }
    }

    std::cout << "Bye!\n";
    return 0;
}
