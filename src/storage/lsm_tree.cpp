#include "storage/lsm_tree.hpp"
#include <filesystem>
#include <algorithm>
#include <regex>

namespace dkv {

LSMTree::LSMTree(const std::string& data_dir, LSMConfig config)
    : data_dir_(data_dir), config_(config) {
    
    std::filesystem::create_directories(data_dir_);
    
    memtable_ = std::make_unique<MemTable>();
    wal_ = std::make_unique<WAL>(data_dir_ + "/wal.log");
    
    loadSSTables();
    recover();
}

LSMTree::~LSMTree() {
    if (!memtable_->empty()) {
        flush();
    }
}

void LSMTree::loadSSTables() {
    std::vector<std::pair<uint64_t, std::string>> sst_files;
    
    for (const auto& entry : std::filesystem::directory_iterator(data_dir_)) {
        std::string filename = entry.path().filename().string();
        if (filename.find("sstable_") == 0 && filename.find(".sst") != std::string::npos) {
            std::regex re("sstable_(\\d+)\\.sst");
            std::smatch match;
            if (std::regex_match(filename, match, re)) {
                uint64_t id = std::stoull(match[1].str());
                sst_files.push_back({id, entry.path().string()});
                if (id >= sstable_id_) {
                    sstable_id_ = id + 1;
                }
            }
        }
    }
    
    std::sort(sst_files.begin(), sst_files.end(),
        [](const auto& a, const auto& b) { return a.first > b.first; });
    
    for (const auto& [id, path] : sst_files) {
        sstables_.push_back(std::make_unique<SSTable>(path));
    }
}

void LSMTree::recover() {
    auto entries = wal_->recover();
    
    for (const auto& entry : entries) {
        switch (entry.op) {
            case OpType::PUT:
                memtable_->put(entry.key, entry.value);
                break;
            case OpType::DELETE:
                memtable_->del(entry.key);
                break;
        }
    }
}

bool LSMTree::put(const std::string& key, const std::string& value) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    wal_->append(OpType::PUT, key, value);
    memtable_->put(key, value);
    
    maybeFlush();
    return true;
}

std::optional<std::string> LSMTree::get(const std::string& key) const {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto memResult = memtable_->get(key);
    if (memResult) {
        if (memResult->deleted) {
            return std::nullopt;
        }
        return memResult->value;
    }
    
    for (const auto& sst : sstables_) {
        if (!sst->mightContain(key)) {
            continue;
        }
        
        auto result = sst->get(key);
        if (result) {
            if (result->deleted) {
                return std::nullopt;
            }
            return result->value;
        }
    }
    
    return std::nullopt;
}

bool LSMTree::del(const std::string& key) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    wal_->append(OpType::DELETE, key);
    memtable_->del(key);
    
    maybeFlush();
    return true;
}

bool LSMTree::contains(const std::string& key) const {
    return get(key).has_value();
}

void LSMTree::maybeFlush() {
    if (memtable_->memoryUsage() >= config_.memtable_size_limit) {
        uint64_t id = nextSSTableId();
        std::string path = SSTable::create(data_dir_, id, *memtable_);
        
        sstables_.insert(sstables_.begin(), std::make_unique<SSTable>(path));
        
        memtable_->clear();
        wal_->checkpoint();
    }
}

void LSMTree::flush() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (memtable_->empty()) {
        return;
    }
    
    uint64_t id = nextSSTableId();
    std::string path = SSTable::create(data_dir_, id, *memtable_);
    
    sstables_.insert(sstables_.begin(), std::make_unique<SSTable>(path));
    
    memtable_->clear();
    wal_->checkpoint();
}

void LSMTree::sync() {
    std::lock_guard<std::mutex> lock(mutex_);
    wal_->sync();
}

uint64_t LSMTree::nextSSTableId() {
    return sstable_id_++;
}

size_t LSMTree::memtableSize() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return memtable_->memoryUsage();
}

size_t LSMTree::sstableCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return sstables_.size();
}

} // namespace dkv
