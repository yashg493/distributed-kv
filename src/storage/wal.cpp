#include "storage/wal.hpp"
#include <stdexcept>

namespace dkv {

WAL::WAL(const std::string& path) : path_(path) {
    file_.open(path_, std::ios::in | std::ios::out | std::ios::binary | std::ios::app);
    if (!file_.is_open()) {
        file_.open(path_, std::ios::out | std::ios::binary);
        file_.close();
        file_.open(path_, std::ios::in | std::ios::out | std::ios::binary | std::ios::app);
    }
    if (!file_.is_open()) {
        throw std::runtime_error("Failed to open WAL file: " + path_);
    }
}

WAL::~WAL() {
    if (file_.is_open()) {
        file_.flush();
        file_.close();
    }
}

bool WAL::append(OpType op, const std::string& key, const std::string& value) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    LogEntry entry{op, key, value};
    writeEntry(entry);
    file_.flush();
    
    return true;
}

void WAL::writeEntry(const LogEntry& entry) {
    uint8_t op = static_cast<uint8_t>(entry.op);
    file_.write(reinterpret_cast<const char*>(&op), sizeof(op));

    uint32_t keyLen = static_cast<uint32_t>(entry.key.size());
    file_.write(reinterpret_cast<const char*>(&keyLen), sizeof(keyLen));
    file_.write(entry.key.data(), keyLen);

    uint32_t valLen = static_cast<uint32_t>(entry.value.size());
    file_.write(reinterpret_cast<const char*>(&valLen), sizeof(valLen));
    file_.write(entry.value.data(), valLen);
}

std::vector<LogEntry> WAL::recover() {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<LogEntry> entries;

    file_.seekg(0, std::ios::beg);
    file_.clear();

    while (hasMore()) {
        try {
            entries.push_back(readEntry());
        } catch (...) {
            break;
        }
    }

    file_.clear();
    file_.seekp(0, std::ios::end);

    return entries;
}

LogEntry WAL::readEntry() {
    LogEntry entry;

    uint8_t op;
    file_.read(reinterpret_cast<char*>(&op), sizeof(op));
    if (file_.gcount() != sizeof(op)) {
        throw std::runtime_error("Failed to read op");
    }
    entry.op = static_cast<OpType>(op);

    uint32_t keyLen;
    file_.read(reinterpret_cast<char*>(&keyLen), sizeof(keyLen));
    if (file_.gcount() != sizeof(keyLen)) {
        throw std::runtime_error("Failed to read key length");
    }
    entry.key.resize(keyLen);
    file_.read(entry.key.data(), keyLen);

    uint32_t valLen;
    file_.read(reinterpret_cast<char*>(&valLen), sizeof(valLen));
    if (file_.gcount() != sizeof(valLen)) {
        throw std::runtime_error("Failed to read value length");
    }
    entry.value.resize(valLen);
    file_.read(entry.value.data(), valLen);

    return entry;
}

bool WAL::hasMore() {
    auto pos = file_.tellg();
    char c;
    if (!file_.get(c)) {
        return false;
    }
    file_.seekg(pos);
    return true;
}

void WAL::checkpoint() {
    std::lock_guard<std::mutex> lock(mutex_);
    file_.close();
    
    std::ofstream clear(path_, std::ios::binary | std::ios::trunc);
    clear.close();
    
    file_.open(path_, std::ios::in | std::ios::out | std::ios::binary | std::ios::app);
}

void WAL::sync() {
    std::lock_guard<std::mutex> lock(mutex_);
    file_.flush();
}

} // namespace dkv
