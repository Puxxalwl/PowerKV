#pragma once

#include <cstdint>
#include <atomic>
#include <string_view>
#include <vector>
#include <shared_mutex>
#include <optional>

constexpr uint64_t PKV_MAGIC = 0x504F5745524B56;

struct alignas(64) FileHeader {
    uint64_t magic;
    uint64_t total_size;
    uint32_t num_shards;
    uint32_t version;
    uint8_t  reserved[32];
};

struct LogEntryHeader {
    uint32_t key_len;
    uint32_t val_len;
    uint64_t expiry; // 0 = inginity
    uint8_t  flags;  // bit 0: delete, bit 1: compressed
};

class PowerKV {
public:
    PowerKV(const char* path, uint64_t size = 1024 * 1024 * 1024); // 1GB default
    ~PowerKV();

    bool Set(std::string_view key, std::string_view val, uint64_t ttl_sec = 0);
    std::optional<std::string_view> Get(std::string_view key);
    bool Del(std::string_view key);
    bool SetJson(std::string_view key, const std::string& json_body, uint64_t ttl_sec = 0);

private:
    struct IndexEntry {
        uint64_t key_hash;
        uint64_t offset;
        uint32_t key_len;    
        uint32_t val_len;
        uint64_t expiry;
    };

    struct alignas(64) Shard {
        std::shared_mutex mutex;
        IndexEntry* index_table;
        uint64_t index_cap;
        std::atomic<uint64_t> write_cursor; 
    };

    int fd;
    char* map_addr;
    uint64_t total_size;
    FileHeader* header;    
    std::vector<Shard> shards;
    uint32_t shard_mask;
    static uint64_t Hash(const void* data, size_t len);
    Shard& GetShard(uint64_t hash);
    void InitShards();
};