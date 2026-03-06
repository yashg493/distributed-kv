#include "replication/replication_log.hpp"
#include <chrono>
#include <filesystem>
#include <stdexcept>
#include <algorithm>

namespace dkv {

ReplicationLog::ReplicationLog(const std::string& log_path)
    : log_path_(log_path) {
    
    // Create parent directory if needed
    std::filesystem::path path(log_path);
    if (path.has_parent_path()) {
        std::filesystem::create_directories(path.parent_path());
    }
    
    // Load existing entries
    loadFromDisk();
    
    // Open log file for appending
    log_file_.open(log_path_, std::ios::binary | std::ios::app);
    if (!log_file_) {
        throw std::runtime_error("Failed to open replication log: " + log_path_);
    }
}

ReplicationLog::~ReplicationLog() {
    if (log_file_.is_open()) {
        log_file_.close();
    }
}

uint64_t ReplicationLog::append(OpCode op, const std::string& key, const std::string& value) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    ReplicationEntry entry;
    entry.sequence_num = next_seq_++;
    entry.op = op;
    entry.key = key;
    entry.value = value;
    entry.timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();
    
    entries_.push_back(entry);
    appendToDisk(entry);
    
    return entry.sequence_num;
}

std::vector<ReplicationEntry> ReplicationLog::getEntriesSince(uint64_t seq_num) const {
    std::lock_guard<std::mutex> lock(mutex_);
    
    std::vector<ReplicationEntry> result;
    for (const auto& entry : entries_) {
        if (entry.sequence_num > seq_num) {
            result.push_back(entry);
        }
    }
    return result;
}

std::optional<ReplicationEntry> ReplicationLog::getEntry(uint64_t seq_num) const {
    std::lock_guard<std::mutex> lock(mutex_);
    
    for (const auto& entry : entries_) {
        if (entry.sequence_num == seq_num) {
            return entry;
        }
    }
    return std::nullopt;
}

uint64_t ReplicationLog::getLastSequence() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return next_seq_ - 1;
}

size_t ReplicationLog::size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return entries_.size();
}

void ReplicationLog::truncateBefore(uint64_t seq_num) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Remove entries with sequence < seq_num
    entries_.erase(
        std::remove_if(entries_.begin(), entries_.end(),
            [seq_num](const ReplicationEntry& e) {
                return e.sequence_num < seq_num;
            }),
        entries_.end()
    );
    
    // Rewrite the log file
    rewriteLog();
}

void ReplicationLog::sync() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (log_file_.is_open()) {
        log_file_.flush();
    }
}

void ReplicationLog::loadFromDisk() {
    std::ifstream file(log_path_, std::ios::binary);
    if (!file) {
        return;  // No existing log
    }
    
    while (file.peek() != EOF) {
        // Read entry length (4 bytes)
        uint32_t len = 0;
        file.read(reinterpret_cast<char*>(&len), 4);
        if (!file || len == 0) break;
        
        // Read entry data
        std::vector<uint8_t> data(len);
        file.read(reinterpret_cast<char*>(data.data()), len);
        if (!file) break;
        
        try {
            ReplicationEntry entry = ReplicationEntry::deserialize(data);
            entries_.push_back(entry);
            if (entry.sequence_num >= next_seq_) {
                next_seq_ = entry.sequence_num + 1;
            }
        } catch (const std::exception&) {
            // Corrupted entry, stop loading
            break;
        }
    }
}

void ReplicationLog::appendToDisk(const ReplicationEntry& entry) {
    if (!log_file_.is_open()) return;
    
    std::vector<uint8_t> data = entry.serialize();
    uint32_t len = static_cast<uint32_t>(data.size());
    
    log_file_.write(reinterpret_cast<const char*>(&len), 4);
    log_file_.write(reinterpret_cast<const char*>(data.data()), data.size());
    log_file_.flush();
}

void ReplicationLog::rewriteLog() {
    // Close current file
    if (log_file_.is_open()) {
        log_file_.close();
    }
    
    // Rewrite with remaining entries
    std::ofstream file(log_path_, std::ios::binary | std::ios::trunc);
    for (const auto& entry : entries_) {
        std::vector<uint8_t> data = entry.serialize();
        uint32_t len = static_cast<uint32_t>(data.size());
        file.write(reinterpret_cast<const char*>(&len), 4);
        file.write(reinterpret_cast<const char*>(data.data()), data.size());
    }
    file.close();
    
    // Reopen for appending
    log_file_.open(log_path_, std::ios::binary | std::ios::app);
}

} // namespace dkv
