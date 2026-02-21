# Distributed Key-Value Store

A distributed, fault-tolerant key-value store built from scratch in C++17.

## Features

- Thread-safe in-memory storage with reader-writer locks
- O(1) average time complexity for get/put/delete
- Concurrent read access support


- In-memory thread-safe KV store
- Write-Ahead Log (WAL)
- LSM Tree storage engine
- Networking (TCP/gRPC)
- Raft consensus
- Sharding

## Build

```bash
mkdir build && cd build
cmake ..
cmake --build .
```

## Run

```bash
./kv_test
```

## API

```cpp
#include "storage/kv_store.hpp"

dkv::KVStore store;

store.put("key", "value");       // returns true if inserted, false if updated
auto val = store.get("key");     // returns std::optional<std::string>
store.del("key");                // returns true if deleted
store.contains("key");           // returns bool
store.size();                    // returns size_t
store.keys();                    // returns std::vector<std::string>
store.clear();                   // clears all data
```

## Project Structure

```
distributed-kv/
├── CMakeLists.txt
├── include/
│   └── storage/
│       └── kv_store.hpp
├── src/
│   ├── storage/
│   │   └── kv_store.cpp
│   └── main.cpp
└── tests/
```

## License

MIT
