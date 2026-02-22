#include "storage/persistent_kv_store.hpp"
#include <filesystem>

namespace dkv {

PersistentKVStore::PersistentKVStore(const std::string& data_dir) 
    : data_dir_(data_dir) {
    
    std::filesystem::create_directories(data_dir_);
    
    std::string wal_path = data_dir_ + "/wal.log";
    wal_ = std::make_unique<WAL>(wal_path);
    
    recover();
}

void PersistentKVStore::recover() {
    auto entries = wal_->recover();
    
    for (const auto& entry : entries) {
        switch (entry.op) {
            case OpType::PUT:
                store_.put(entry.key, entry.value);
                break;
            case OpType::DELETE:
                store_.del(entry.key);
                break;
        }
    }
}

bool PersistentKVStore::put(const std::string& key, const std::string& value) {
    wal_->append(OpType::PUT, key, value);
    return store_.put(key, value);
}

std::optional<std::string> PersistentKVStore::get(const std::string& key) const {
    return store_.get(key);
}

bool PersistentKVStore::del(const std::string& key) {
    wal_->append(OpType::DELETE, key);
    return store_.del(key);
}

bool PersistentKVStore::contains(const std::string& key) const {
    return store_.contains(key);
}

size_t PersistentKVStore::size() const {
    return store_.size();
}

std::vector<std::string> PersistentKVStore::keys() const {
    return store_.keys();
}

void PersistentKVStore::clear() {
    store_.clear();
    wal_->checkpoint();
}

void PersistentKVStore::checkpoint() {
    wal_->checkpoint();
}

void PersistentKVStore::sync() {
    wal_->sync();
}

} // namespace dkv
