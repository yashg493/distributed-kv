#pragma once

#include <string>
#include <optional>
#include <unordered_map>
#include <shared_mutex>
#include <mutex>
#include <vector>

namespace dkv {

class KVStore {
public:
    KVStore() = default;
    ~KVStore() = default;

    KVStore(const KVStore&) = delete;
    KVStore& operator=(const KVStore&) = delete;
    KVStore(KVStore&&) = default;
    KVStore& operator=(KVStore&&) = default;

    bool put(const std::string& key, const std::string& value);
    std::optional<std::string> get(const std::string& key) const;
    bool del(const std::string& key);
    bool contains(const std::string& key) const;
    size_t size() const;
    std::vector<std::string> keys() const;
    void clear();

private:
    std::unordered_map<std::string, std::string> data_;
    mutable std::shared_mutex mutex_;
};

} // namespace dkv
