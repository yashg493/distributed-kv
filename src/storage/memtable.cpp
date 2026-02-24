#include "storage/memtable.hpp"

namespace dkv {

void MemTable::put(const std::string& key, const std::string& value) {
    std::unique_lock lock(mutex_);
    
    auto it = data_.find(key);
    if (it != data_.end()) {
        memory_usage_ -= it->second.value.size();
        it->second = {value, false};
    } else {
        data_[key] = {value, false};
        memory_usage_ += key.size();
    }
    memory_usage_ += value.size();
}

void MemTable::del(const std::string& key) {
    std::unique_lock lock(mutex_);
    
    auto it = data_.find(key);
    if (it != data_.end()) {
        memory_usage_ -= it->second.value.size();
        it->second = {"", true};
    } else {
        data_[key] = {"", true};
        memory_usage_ += key.size();
    }
}

std::optional<MemTableEntry> MemTable::get(const std::string& key) const {
    std::shared_lock lock(mutex_);
    
    auto it = data_.find(key);
    if (it != data_.end()) {
        return it->second;
    }
    return std::nullopt;
}

bool MemTable::contains(const std::string& key) const {
    std::shared_lock lock(mutex_);
    return data_.count(key) > 0;
}

size_t MemTable::size() const {
    std::shared_lock lock(mutex_);
    return data_.size();
}

size_t MemTable::memoryUsage() const {
    std::shared_lock lock(mutex_);
    return memory_usage_;
}

bool MemTable::empty() const {
    std::shared_lock lock(mutex_);
    return data_.empty();
}

void MemTable::clear() {
    std::unique_lock lock(mutex_);
    data_.clear();
    memory_usage_ = 0;
}

} // namespace dkv
