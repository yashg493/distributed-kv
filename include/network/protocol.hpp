#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace dkv {

enum class OpCode : uint8_t {
    OP_PUT = 1,
    OP_GET = 2,
    OP_DELETE = 3,
    OP_PING = 4
};

enum class StatusCode : uint8_t {
    STATUS_OK = 0,
    STATUS_NOT_FOUND = 1,
    STATUS_ERROR = 2
};

struct Request {
    OpCode op;
    std::string key;
    std::string value;
    
    std::vector<uint8_t> serialize() const;
    static Request deserialize(const std::vector<uint8_t>& data);
};

struct Response {
    StatusCode status;
    std::string value;
    std::string error;
    
    std::vector<uint8_t> serialize() const;
    static Response deserialize(const std::vector<uint8_t>& data);
};

} // namespace dkv
