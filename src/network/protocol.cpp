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

} // namespace dkv
