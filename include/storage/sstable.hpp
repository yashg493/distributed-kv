#pragma once

#include <string>
#include <optional>
#include <vector>
#include <fstream>
#include <cstdint>
#include "storage/memtable.hpp"

namespace dkv {

struct SSTableEntry {
    std::string key;
    std::string value;
    bool deleted;
};

struct IndexEntry {
    std::string key;
    uint64_t offset;
};

class SSTable {
public:
    static std::string create(const std::string& dir, uint64_t id, const MemTable& memtable);
    
    explicit SSTable(const std::string& path);
    
    std::optional<SSTableEntry> get(const std::string& key) const;
    bool mightContain(const std::string& key) const;
    
    const std::string& path() const { return path_; }
    const std::string& minKey() const { return min_key_; }
    const std::string& maxKey() const { return max_key_; }
    size_t entryCount() const { return entry_count_; }

private:
    void loadIndex();
    std::optional<SSTableEntry> readEntryAt(uint64_t offset) const;
    uint64_t findOffset(const std::string& key) const;

    std::string path_;
    std::vector<IndexEntry> index_;
    std::string min_key_;
    std::string max_key_;
    size_t entry_count_ = 0;
    
    static constexpr size_t INDEX_INTERVAL = 16;
};

} // namespace dkv
