#include "storage/kv_store.hpp"

namespace dkv {

bool KVStore::put(const std::string& key, const std::string& value) {
    std::unique_lock lock(mutex_);
    auto [it, inserted] = data_.insert_or_assign(key, value);
    return inserted;
}

std::optional<std::string> KVStore::get(const std::string& key) const {
    std::shared_lock lock(mutex_);
    auto it = data_.find(key);
    if (it != data_.end()) {
        return it->second;
    }
    return std::nullopt;
}

bool KVStore::del(const std::string& key) {
    std::unique_lock lock(mutex_);
    return data_.erase(key) > 0;
}

bool KVStore::contains(const std::string& key) const {
    std::shared_lock lock(mutex_);
    return data_.count(key) > 0;
}

size_t KVStore::size() const {
    std::shared_lock lock(mutex_);
    return data_.size();
}

std::vector<std::string> KVStore::keys() const {
    std::shared_lock lock(mutex_);
    std::vector<std::string> result;
    result.reserve(data_.size());
    for (const auto& [key, value] : data_) {
        result.push_back(key);
    }
    return result;
}

void KVStore::clear() {
    std::unique_lock lock(mutex_);
    data_.clear();
}

} // namespace dkv
