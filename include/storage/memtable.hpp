#pragma once

#include <string>
#include <optional>
#include <map>
#include <shared_mutex>
#include <mutex>
#include <cstdint>

namespace dkv {

struct MemTableEntry {
    std::string value;
    bool deleted;
};

class MemTable {
public:
    MemTable() = default;

    void put(const std::string& key, const std::string& value);
    void del(const std::string& key);
    std::optional<MemTableEntry> get(const std::string& key) const;
    bool contains(const std::string& key) const;

    size_t size() const;
    size_t memoryUsage() const;
    bool empty() const;
    void clear();

    using Iterator = std::map<std::string, MemTableEntry>::const_iterator;
    Iterator begin() const { return data_.begin(); }
    Iterator end() const { return data_.end(); }

private:
    std::map<std::string, MemTableEntry> data_;
    mutable std::shared_mutex mutex_;
    size_t memory_usage_ = 0;
};

} // namespace dkv
