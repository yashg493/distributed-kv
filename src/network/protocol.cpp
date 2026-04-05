#include "network/protocol.hpp"
#include <cstring>
#include <stdexcept>

namespace dkv {

std::vector<uint8_t> Request::serialize() const {
    std::vector<uint8_t> data;
    
    data.push_back(static_cast<uint8_t>(op));
    
    uint32_t keyLen = static_cast<uint32_t>(key.size());
    data.push_back((keyLen >> 0) & 0xFF);
    data.push_back((keyLen >> 8) & 0xFF);
    data.push_back((keyLen >> 16) & 0xFF);
    data.push_back((keyLen >> 24) & 0xFF);
    data.insert(data.end(), key.begin(), key.end());
    
    uint32_t valLen = static_cast<uint32_t>(value.size());
    data.push_back((valLen >> 0) & 0xFF);
    data.push_back((valLen >> 8) & 0xFF);
    data.push_back((valLen >> 16) & 0xFF);
    data.push_back((valLen >> 24) & 0xFF);
    data.insert(data.end(), value.begin(), value.end());
    
    return data;
}

Request Request::deserialize(const std::vector<uint8_t>& data) {
    if (data.size() < 9) {
        throw std::runtime_error("Invalid request: too short");
    }
    
    Request req;
    size_t offset = 0;
    
    req.op = static_cast<OpCode>(data[offset++]);
    
    uint32_t keyLen = data[offset] | (data[offset+1] << 8) | 
                      (data[offset+2] << 16) | (data[offset+3] << 24);
    offset += 4;
    
    if (offset + keyLen > data.size()) {
        throw std::runtime_error("Invalid request: key length mismatch");
    }
    req.key = std::string(data.begin() + offset, data.begin() + offset + keyLen);
    offset += keyLen;
    
    if (offset + 4 > data.size()) {
        throw std::runtime_error("Invalid request: missing value length");
    }
    uint32_t valLen = data[offset] | (data[offset+1] << 8) | 
                      (data[offset+2] << 16) | (data[offset+3] << 24);
    offset += 4;
    
    if (offset + valLen > data.size()) {
        throw std::runtime_error("Invalid request: value length mismatch");
    }
    req.value = std::string(data.begin() + offset, data.begin() + offset + valLen);
    
    return req;
}

std::vector<uint8_t> Response::serialize() const {
    std::vector<uint8_t> data;
    
    data.push_back(static_cast<uint8_t>(status));
    
    uint32_t valLen = static_cast<uint32_t>(value.size());
    data.push_back((valLen >> 0) & 0xFF);
    data.push_back((valLen >> 8) & 0xFF);
    data.push_back((valLen >> 16) & 0xFF);
    data.push_back((valLen >> 24) & 0xFF);
    data.insert(data.end(), value.begin(), value.end());
    
    uint32_t errLen = static_cast<uint32_t>(error.size());
    data.push_back((errLen >> 0) & 0xFF);
    data.push_back((errLen >> 8) & 0xFF);
    data.push_back((errLen >> 16) & 0xFF);
    data.push_back((errLen >> 24) & 0xFF);
    data.insert(data.end(), error.begin(), error.end());
    
    return data;
}

Response Response::deserialize(const std::vector<uint8_t>& data) {
    if (data.size() < 9) {
        throw std::runtime_error("Invalid response: too short");
    }
    
    Response resp;
    size_t offset = 0;
    
    resp.status = static_cast<StatusCode>(data[offset++]);
    
    uint32_t valLen = data[offset] | (data[offset+1] << 8) | 
                      (data[offset+2] << 16) | (data[offset+3] << 24);
    offset += 4;
    
    if (offset + valLen > data.size()) {
        throw std::runtime_error("Invalid response: value length mismatch");
    }
    resp.value = std::string(data.begin() + offset, data.begin() + offset + valLen);
    offset += valLen;
    
    if (offset + 4 > data.size()) {
        throw std::runtime_error("Invalid response: missing error length");
    }
    uint32_t errLen = data[offset] | (data[offset+1] << 8) | 
                      (data[offset+2] << 16) | (data[offset+3] << 24);
    offset += 4;
    
    if (offset + errLen > data.size()) {
        throw std::runtime_error("Invalid response: error length mismatch");
    }
    resp.error = std::string(data.begin() + offset, data.begin() + offset + errLen);
    
    return resp;
}

// Helper to write uint64_t in little-endian
static void writeU64(std::vector<uint8_t>& data, uint64_t val) {
    for (int i = 0; i < 8; ++i) {
        data.push_back((val >> (i * 8)) & 0xFF);
    }
}

// Helper to read uint64_t in little-endian
static uint64_t readU64(const std::vector<uint8_t>& data, size_t offset) {
    uint64_t val = 0;
    for (int i = 0; i < 8; ++i) {
        val |= static_cast<uint64_t>(data[offset + i]) << (i * 8);
    }
    return val;
}

std::vector<uint8_t> ReplicationEntry::serialize() const {
    std::vector<uint8_t> data;
    
    // sequence_num (8 bytes)
    writeU64(data, sequence_num);
    
    // op (1 byte)
    data.push_back(static_cast<uint8_t>(op));
    
    // key (4 bytes len + data)
    uint32_t keyLen = static_cast<uint32_t>(key.size());
    data.push_back((keyLen >> 0) & 0xFF);
    data.push_back((keyLen >> 8) & 0xFF);
    data.push_back((keyLen >> 16) & 0xFF);
    data.push_back((keyLen >> 24) & 0xFF);
    data.insert(data.end(), key.begin(), key.end());
    
    // value (4 bytes len + data)
    uint32_t valLen = static_cast<uint32_t>(value.size());
    data.push_back((valLen >> 0) & 0xFF);
    data.push_back((valLen >> 8) & 0xFF);
    data.push_back((valLen >> 16) & 0xFF);
    data.push_back((valLen >> 24) & 0xFF);
    data.insert(data.end(), value.begin(), value.end());
    
    // timestamp (8 bytes)
    writeU64(data, timestamp);
    
    return data;
}

ReplicationEntry ReplicationEntry::deserialize(const std::vector<uint8_t>& data) {
    if (data.size() < 25) {  // 8 + 1 + 4 + 4 + 8 minimum
        throw std::runtime_error("Invalid replication entry: too short");
    }
    
    ReplicationEntry entry;
    size_t offset = 0;
    
    // sequence_num
    entry.sequence_num = readU64(data, offset);
    offset += 8;
    
    // op
    entry.op = static_cast<OpCode>(data[offset++]);
    
    // key
    uint32_t keyLen = data[offset] | (data[offset+1] << 8) | 
                      (data[offset+2] << 16) | (data[offset+3] << 24);
    offset += 4;
    if (offset + keyLen > data.size()) {
        throw std::runtime_error("Invalid replication entry: key length mismatch");
    }
    entry.key = std::string(data.begin() + offset, data.begin() + offset + keyLen);
    offset += keyLen;
    
    // value
    if (offset + 4 > data.size()) {
        throw std::runtime_error("Invalid replication entry: missing value length");
    }
    uint32_t valLen = data[offset] | (data[offset+1] << 8) | 
                      (data[offset+2] << 16) | (data[offset+3] << 24);
    offset += 4;
    if (offset + valLen > data.size()) {
        throw std::runtime_error("Invalid replication entry: value length mismatch");
    }
    entry.value = std::string(data.begin() + offset, data.begin() + offset + valLen);
    offset += valLen;
    
    // timestamp
    if (offset + 8 > data.size()) {
        throw std::runtime_error("Invalid replication entry: missing timestamp");
    }
    entry.timestamp = readU64(data, offset);
    
    return entry;
}

// Helper to write string (4-byte length + data)
static void writeString(std::vector<uint8_t>& data, const std::string& str) {
    uint32_t len = static_cast<uint32_t>(str.size());
    data.push_back((len >> 0) & 0xFF);
    data.push_back((len >> 8) & 0xFF);
    data.push_back((len >> 16) & 0xFF);
    data.push_back((len >> 24) & 0xFF);
    data.insert(data.end(), str.begin(), str.end());
}

// Helper to read string
static std::string readString(const std::vector<uint8_t>& data, size_t& offset) {
    if (offset + 4 > data.size()) {
        throw std::runtime_error("Invalid data: missing string length");
    }
    uint32_t len = data[offset] | (data[offset+1] << 8) | 
                   (data[offset+2] << 16) | (data[offset+3] << 24);
    offset += 4;
    if (offset + len > data.size()) {
        throw std::runtime_error("Invalid data: string length mismatch");
    }
    std::string str(data.begin() + offset, data.begin() + offset + len);
    offset += len;
    return str;
}

// ==================== RaftLogEntry ====================

std::vector<uint8_t> RaftLogEntry::serialize() const {
    std::vector<uint8_t> data;
    writeU64(data, term);
    writeU64(data, index);
    data.push_back(static_cast<uint8_t>(op));
    writeString(data, key);
    writeString(data, value);
    return data;
}

RaftLogEntry RaftLogEntry::deserialize(const std::vector<uint8_t>& data) {
    RaftLogEntry entry;
    size_t offset = 0;
    
    entry.term = readU64(data, offset);
    offset += 8;
    entry.index = readU64(data, offset);
    offset += 8;
    entry.op = static_cast<OpCode>(data[offset++]);
    entry.key = readString(data, offset);
    entry.value = readString(data, offset);
    
    return entry;
}

// ==================== RequestVote ====================

std::vector<uint8_t> RequestVote::serialize() const {
    std::vector<uint8_t> data;
    writeU64(data, term);
    writeString(data, candidate_id);
    writeU64(data, last_log_index);
    writeU64(data, last_log_term);
    return data;
}

RequestVote RequestVote::deserialize(const std::vector<uint8_t>& data) {
    RequestVote rv;
    size_t offset = 0;
    
    rv.term = readU64(data, offset);
    offset += 8;
    rv.candidate_id = readString(data, offset);
    rv.last_log_index = readU64(data, offset);
    offset += 8;
    rv.last_log_term = readU64(data, offset);
    
    return rv;
}

// ==================== RequestVoteResponse ====================

std::vector<uint8_t> RequestVoteResponse::serialize() const {
    std::vector<uint8_t> data;
    writeU64(data, term);
    data.push_back(vote_granted ? 1 : 0);
    return data;
}

RequestVoteResponse RequestVoteResponse::deserialize(const std::vector<uint8_t>& data) {
    RequestVoteResponse rvr;
    size_t offset = 0;
    
    rvr.term = readU64(data, offset);
    offset += 8;
    rvr.vote_granted = (data[offset] != 0);
    
    return rvr;
}

// ==================== AppendEntries ====================

std::vector<uint8_t> AppendEntries::serialize() const {
    std::vector<uint8_t> data;
    writeU64(data, term);
    writeString(data, leader_id);
    writeU64(data, prev_log_index);
    writeU64(data, prev_log_term);
    
    // Entries count
    uint32_t count = static_cast<uint32_t>(entries.size());
    data.push_back((count >> 0) & 0xFF);
    data.push_back((count >> 8) & 0xFF);
    data.push_back((count >> 16) & 0xFF);
    data.push_back((count >> 24) & 0xFF);
    
    // Each entry (length-prefixed)
    for (const auto& entry : entries) {
        auto entry_data = entry.serialize();
        uint32_t entry_len = static_cast<uint32_t>(entry_data.size());
        data.push_back((entry_len >> 0) & 0xFF);
        data.push_back((entry_len >> 8) & 0xFF);
        data.push_back((entry_len >> 16) & 0xFF);
        data.push_back((entry_len >> 24) & 0xFF);
        data.insert(data.end(), entry_data.begin(), entry_data.end());
    }
    
    writeU64(data, leader_commit);
    return data;
}

AppendEntries AppendEntries::deserialize(const std::vector<uint8_t>& data) {
    AppendEntries ae;
    size_t offset = 0;
    
    ae.term = readU64(data, offset);
    offset += 8;
    ae.leader_id = readString(data, offset);
    ae.prev_log_index = readU64(data, offset);
    offset += 8;
    ae.prev_log_term = readU64(data, offset);
    offset += 8;
    
    // Entries count
    uint32_t count = data[offset] | (data[offset+1] << 8) | 
                     (data[offset+2] << 16) | (data[offset+3] << 24);
    offset += 4;
    
    // Each entry
    for (uint32_t i = 0; i < count; ++i) {
        uint32_t entry_len = data[offset] | (data[offset+1] << 8) | 
                             (data[offset+2] << 16) | (data[offset+3] << 24);
        offset += 4;
        std::vector<uint8_t> entry_data(data.begin() + offset, data.begin() + offset + entry_len);
        ae.entries.push_back(RaftLogEntry::deserialize(entry_data));
        offset += entry_len;
    }
    
    ae.leader_commit = readU64(data, offset);
    return ae;
}

// ==================== AppendEntriesResponse ====================

std::vector<uint8_t> AppendEntriesResponse::serialize() const {
    std::vector<uint8_t> data;
    writeU64(data, term);
    data.push_back(success ? 1 : 0);
    writeU64(data, match_index);
    return data;
}

AppendEntriesResponse AppendEntriesResponse::deserialize(const std::vector<uint8_t>& data) {
    AppendEntriesResponse aer;
    size_t offset = 0;
    
    aer.term = readU64(data, offset);
    offset += 8;
    aer.success = (data[offset++] != 0);
    aer.match_index = readU64(data, offset);
    
    return aer;
}

} // namespace dkv
