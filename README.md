# Distributed Key-Value Store

A distributed, fault-tolerant key-value store built from scratch in C++17.

## Features

- Thread-safe in-memory storage with reader-writer locks
- O(1) average time complexity for get/put/delete
- Concurrent read access support
- Write-Ahead Log (WAL) for crash recovery

## Roadmap

In-memory thread-safe KV store
Write-Ahead Log (WAL)
LSM Tree storage engine
Networking (TCP/gRPC)
Raft consensus
Sharding

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

### In-Memory Store
```cpp
#include "storage/kv_store.hpp"

dkv::KVStore store;
store.put("key", "value");
auto val = store.get("key");
store.del("key");
```

### Persistent Store (with WAL)
```cpp
#include "storage/persistent_kv_store.hpp"

dkv::PersistentKVStore store("./data");
store.put("key", "value");    // logged to WAL before memory update
auto val = store.get("key");
store.del("key");
store.checkpoint();           // truncate WAL
```

## Project Structure

```
distributed-kv/
├── CMakeLists.txt
├── include/storage/
│   ├── kv_store.hpp
│   ├── wal.hpp
│   └── persistent_kv_store.hpp
├── src/storage/
│   ├── kv_store.cpp
│   ├── wal.cpp
│   └── persistent_kv_store.cpp
└── src/main.cpp
```

## License

MIT
