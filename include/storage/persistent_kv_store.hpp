#pragma once

#include "storage/kv_store.hpp"
#include "storage/wal.hpp"
#include <memory>

namespace dkv {

class PersistentKVStore {
public:
    explicit PersistentKVStore(const std::string& data_dir);
    ~PersistentKVStore() = default;

    PersistentKVStore(const PersistentKVStore&) = delete;
    PersistentKVStore& operator=(const PersistentKVStore&) = delete;

    bool put(const std::string& key, const std::string& value);
    std::optional<std::string> get(const std::string& key) const;
    bool del(const std::string& key);
    bool contains(const std::string& key) const;
    size_t size() const;
    std::vector<std::string> keys() const;
    void clear();
    
    void checkpoint();
    void sync();

private:
    void recover();

    std::string data_dir_;
    KVStore store_;
    std::unique_ptr<WAL> wal_;
};

} // namespace dkv
