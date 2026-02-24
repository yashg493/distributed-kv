#pragma once

#include <string>
#include <optional>
#include <vector>
#include <memory>
#include <mutex>
#include <atomic>
#include "storage/memtable.hpp"
#include "storage/sstable.hpp"
#include "storage/wal.hpp"

namespace dkv {

struct LSMConfig {
    size_t memtable_size_limit = 4 * 1024 * 1024;  // 4MB
    size_t max_sstables = 10;
};

class LSMTree {
public:
    explicit LSMTree(const std::string& data_dir, LSMConfig config = {});
    ~LSMTree();

    LSMTree(const LSMTree&) = delete;
    LSMTree& operator=(const LSMTree&) = delete;

    bool put(const std::string& key, const std::string& value);
    std::optional<std::string> get(const std::string& key) const;
    bool del(const std::string& key);
    bool contains(const std::string& key) const;

    void flush();
    void sync();
    
    size_t memtableSize() const;
    size_t sstableCount() const;

private:
    void recover();
    void maybeFlush();
    void loadSSTables();
    uint64_t nextSSTableId();

    std::string data_dir_;
    LSMConfig config_;
    
    std::unique_ptr<MemTable> memtable_;
    std::unique_ptr<WAL> wal_;
    std::vector<std::unique_ptr<SSTable>> sstables_;
    
    mutable std::mutex mutex_;
    std::atomic<uint64_t> sstable_id_{0};
};

} // namespace dkv
