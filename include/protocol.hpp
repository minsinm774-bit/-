#ifndef GATEWAY_PROTOCOL_H
#define GATEWAY_PROTOCOL_H

#include <cstdint>

// 与 Java 对齐：两段 uint32_t 均为 **big-endian（网络字节序）**，
// Java 侧用 ByteOrder.BIG_ENDIAN 读取。
#pragma pack(push, 1)
struct Envelope {
    uint32_t payload_len; /**< 紧随其后原始载荷的字节数（network byte order） */
    uint32_t client_fd;   /**< 来源 TCP 连接在 C++ 侧的 fd（network byte order） */
};
#pragma pack(pop)

static_assert(sizeof(Envelope) == 8, "Envelope wire size must be 8 bytes");

#endif
