#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <mutex>
#include <fstream>
#include <optional>
#include "network/protocol.hpp"

namespace dkv {

/**
 * ReplicationLog - Append-only log of write operations for leader-follower replication.
 * 
 * Each entry has a monotonically increasing sequence number.
 * The log is persisted to disk for crash recovery.
 */
class ReplicationLog {
public:
    explicit ReplicationLog(const std::string& log_path);
    ~ReplicationLog();

    ReplicationLog(const ReplicationLog&) = delete;
    ReplicationLog& operator=(const ReplicationLog&) = delete;

    // Append a new entry and return its sequence number
    uint64_t append(OpCode op, const std::string& key, const std::string& value);
    
    // Get all entries since (but not including) the given sequence number
    std::vector<ReplicationEntry> getEntriesSince(uint64_t seq_num) const;
    
    // Get a specific entry by sequence number
    std::optional<ReplicationEntry> getEntry(uint64_t seq_num) const;
    
    // Get the last sequence number (0 if empty)
    uint64_t getLastSequence() const;
    
    // Get total number of entries
    size_t size() const;
    
    // Truncate log entries before the given sequence number
    // Used after all followers have caught up
    void truncateBefore(uint64_t seq_num);
    
    // Sync to disk
    void sync();

private:
    void loadFromDisk();
    void appendToDisk(const ReplicationEntry& entry);
    void rewriteLog();  // Rewrite after truncation
    
    std::string log_path_;
    std::vector<ReplicationEntry> entries_;
    uint64_t next_seq_ = 1;
    mutable std::mutex mutex_;
    std::ofstream log_file_;
};

} // namespace dkv
