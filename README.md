# Distributed Key-Value Store

A distributed, fault-tolerant key-value store built from scratch in C++17.

## Features

- Thread-safe in-memory storage with reader-writer locks
- O(1) average time complexity for get/put/delete
- Write-Ahead Log (WAL) for crash recovery
- LSM Tree for datasets larger than memory

## Roadmap

- 1.1: In-memory thread-safe KV store
- 1.2: Write-Ahead Log (WAL)
- 1.3: LSM Tree storage engine
- 2: Networking (TCP/gRPC)
- 3: Raft consensus
- 4: Sharding

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
store.put("key", "value");
auto val = store.get("key");
store.checkpoint();
```

### LSM Tree (handles large datasets)
```cpp
#include "storage/lsm_tree.hpp"

dkv::LSMConfig config;
config.memtable_size_limit = 4 * 1024 * 1024;  // 4MB
dkv::LSMTree lsm("./data", config);

lsm.put("key", "value");  // writes to memtable + WAL
auto val = lsm.get("key");  // checks memtable, then SSTables
lsm.flush();  // force flush memtable to SSTable
```

## Project Structure

```
distributed-kv/
├── CMakeLists.txt
├── include/storage/
│   ├── kv_store.hpp
│   ├── wal.hpp
│   ├── memtable.hpp
│   ├── sstable.hpp
│   └── lsm_tree.hpp
├── src/storage/
│   ├── kv_store.cpp
│   ├── wal.cpp
│   ├── memtable.cpp
│   ├── sstable.cpp
│   └── lsm_tree.cpp
└── src/main.cpp
```

## License

MIT
