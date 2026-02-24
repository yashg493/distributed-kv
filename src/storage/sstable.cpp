#include "storage/sstable.hpp"
#include <algorithm>
#include <stdexcept>

namespace dkv {

std::string SSTable::create(const std::string& dir, uint64_t id, const MemTable& memtable) {
    std::string path = dir + "/sstable_" + std::to_string(id) + ".sst";
    std::ofstream file(path, std::ios::binary);
    
    if (!file.is_open()) {
        throw std::runtime_error("Failed to create SSTable: " + path);
    }

    std::vector<IndexEntry> index;
    size_t count = 0;
    uint64_t offset = 0;

    for (auto it = memtable.begin(); it != memtable.end(); ++it) {
        if (count % INDEX_INTERVAL == 0) {
            index.push_back({it->first, offset});
        }

        uint8_t deleted = it->second.deleted ? 1 : 0;
        file.write(reinterpret_cast<const char*>(&deleted), sizeof(deleted));

        uint32_t keyLen = static_cast<uint32_t>(it->first.size());
        file.write(reinterpret_cast<const char*>(&keyLen), sizeof(keyLen));
        file.write(it->first.data(), keyLen);

        uint32_t valLen = static_cast<uint32_t>(it->second.value.size());
        file.write(reinterpret_cast<const char*>(&valLen), sizeof(valLen));
        file.write(it->second.value.data(), valLen);

        offset = file.tellp();
        count++;
    }

    uint64_t indexOffset = offset;
    uint32_t indexSize = static_cast<uint32_t>(index.size());
    file.write(reinterpret_cast<const char*>(&indexSize), sizeof(indexSize));

    for (const auto& entry : index) {
        uint32_t keyLen = static_cast<uint32_t>(entry.key.size());
        file.write(reinterpret_cast<const char*>(&keyLen), sizeof(keyLen));
        file.write(entry.key.data(), keyLen);
        file.write(reinterpret_cast<const char*>(&entry.offset), sizeof(entry.offset));
    }

    file.write(reinterpret_cast<const char*>(&indexOffset), sizeof(indexOffset));
    file.write(reinterpret_cast<const char*>(&count), sizeof(count));

    file.close();
    return path;
}

SSTable::SSTable(const std::string& path) : path_(path) {
    loadIndex();
}

void SSTable::loadIndex() {
    std::ifstream file(path_, std::ios::binary);
    if (!file.is_open()) {
        throw std::runtime_error("Failed to open SSTable: " + path_);
    }

    file.seekg(-static_cast<int>(sizeof(uint64_t) + sizeof(size_t)), std::ios::end);
    
    uint64_t indexOffset;
    file.read(reinterpret_cast<char*>(&indexOffset), sizeof(indexOffset));
    file.read(reinterpret_cast<char*>(&entry_count_), sizeof(entry_count_));

    file.seekg(indexOffset);
    
    uint32_t indexSize;
    file.read(reinterpret_cast<char*>(&indexSize), sizeof(indexSize));

    index_.reserve(indexSize);
    for (uint32_t i = 0; i < indexSize; i++) {
        IndexEntry entry;
        
        uint32_t keyLen;
        file.read(reinterpret_cast<char*>(&keyLen), sizeof(keyLen));
        entry.key.resize(keyLen);
        file.read(entry.key.data(), keyLen);
        file.read(reinterpret_cast<char*>(&entry.offset), sizeof(entry.offset));
        
        index_.push_back(std::move(entry));
    }

    if (!index_.empty()) {
        auto firstEntry = readEntryAt(0);
        if (firstEntry) min_key_ = firstEntry->key;
        
        // Find last entry by scanning from last index position
        file.seekg(index_.back().offset);
        std::string lastKey;
        while (file.tellg() < static_cast<std::streampos>(indexOffset)) {
            uint8_t deleted;
            if (!file.read(reinterpret_cast<char*>(&deleted), sizeof(deleted))) break;
            
            uint32_t keyLen;
            if (!file.read(reinterpret_cast<char*>(&keyLen), sizeof(keyLen))) break;
            
            lastKey.resize(keyLen);
            if (!file.read(lastKey.data(), keyLen)) break;
            
            uint32_t valLen;
            if (!file.read(reinterpret_cast<char*>(&valLen), sizeof(valLen))) break;
            file.seekg(valLen, std::ios::cur);
        }
        if (!lastKey.empty()) max_key_ = lastKey;
    }
}

std::optional<SSTableEntry> SSTable::get(const std::string& key) const {
    if (index_.empty()) return std::nullopt;
    if (key < min_key_ || key > max_key_) return std::nullopt;

    uint64_t startOffset = findOffset(key);
    
    std::ifstream file(path_, std::ios::binary);
    file.seekg(startOffset);

    for (size_t i = 0; i < INDEX_INTERVAL + 1 && file.peek() != EOF; i++) {
        uint64_t currentOffset = file.tellg();
        auto entry = readEntryAt(currentOffset);
        
        if (!entry) break;
        
        if (entry->key == key) {
            return entry;
        }
        if (entry->key > key) {
            break;
        }
        
        file.seekg(currentOffset);
        uint8_t deleted;
        file.read(reinterpret_cast<char*>(&deleted), sizeof(deleted));
        uint32_t keyLen, valLen;
        file.read(reinterpret_cast<char*>(&keyLen), sizeof(keyLen));
        file.seekg(keyLen, std::ios::cur);
        file.read(reinterpret_cast<char*>(&valLen), sizeof(valLen));
        file.seekg(valLen, std::ios::cur);
    }

    return std::nullopt;
}

std::optional<SSTableEntry> SSTable::readEntryAt(uint64_t offset) const {
    std::ifstream file(path_, std::ios::binary);
    file.seekg(offset);

    SSTableEntry entry;
    
    uint8_t deleted;
    if (!file.read(reinterpret_cast<char*>(&deleted), sizeof(deleted))) {
        return std::nullopt;
    }
    entry.deleted = deleted != 0;

    uint32_t keyLen;
    if (!file.read(reinterpret_cast<char*>(&keyLen), sizeof(keyLen))) {
        return std::nullopt;
    }
    entry.key.resize(keyLen);
    if (!file.read(entry.key.data(), keyLen)) {
        return std::nullopt;
    }

    uint32_t valLen;
    if (!file.read(reinterpret_cast<char*>(&valLen), sizeof(valLen))) {
        return std::nullopt;
    }
    entry.value.resize(valLen);
    if (!file.read(entry.value.data(), valLen)) {
        return std::nullopt;
    }

    return entry;
}

uint64_t SSTable::findOffset(const std::string& key) const {
    auto it = std::upper_bound(index_.begin(), index_.end(), key,
        [](const std::string& k, const IndexEntry& e) { return k < e.key; });
    
    if (it == index_.begin()) {
        return 0;
    }
    return std::prev(it)->offset;
}

bool SSTable::mightContain(const std::string& key) const {
    if (index_.empty()) return false;
    return key >= min_key_ && key <= max_key_;
}

} // namespace dkv
