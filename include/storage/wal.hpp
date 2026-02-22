#pragma once

#include <string>
#include <fstream>
#include <vector>
#include <mutex>
#include <cstdint>

namespace dkv {

enum class OpType : uint8_t {
    PUT = 1,
    DELETE = 2
};

struct LogEntry {
    OpType op;
    std::string key;
    std::string value;
};

class WAL {
public:
    explicit WAL(const std::string& path);
    ~WAL();

    WAL(const WAL&) = delete;
    WAL& operator=(const WAL&) = delete;

    bool append(OpType op, const std::string& key, const std::string& value = "");
    std::vector<LogEntry> recover();
    void checkpoint();
    void sync();

private:
    void writeEntry(const LogEntry& entry);
    LogEntry readEntry();
    bool hasMore();

    std::string path_;
    std::fstream file_;
    std::mutex mutex_;
};

} // namespace dkv
