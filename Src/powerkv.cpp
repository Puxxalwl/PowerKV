#include "powerkv.h"
#include <mutex>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <stdexcept>
#include <ctime>

static inline uint64_t _wymum(uint64_t A, uint64_t B) {
    __int128 r = A; r *= B;
    return (uint64_t)r ^ (uint64_t)(r >> 64);
}
uint64_t PowerKV::Hash(const void* key, size_t len) {
    const uint8_t* p = (const uint8_t*)key; 
    uint64_t seed = 0x9E3779B97F4A7C15ULL;
    for (size_t i = 0; i + 8 <= len; i += 8) {
        uint64_t v; memcpy(&v, p + i, 8);
        seed = _wymum(seed ^ v, 0x53c5ca59ULL);
    }
    return _wymum(seed, len ^ 0x53c5ca59ULL);
}


PowerKV::PowerKV(const char* path, uint64_t size) : total_size(size) {
    fd = open(path, O_RDWR | O_CREAT, 0666);
    if (fd == -1) throw std::runtime_error("File open error");
    struct stat st;
    fstat(fd, &st);
    bool is_new = (st.st_size == 0);
    if (ftruncate(fd, size) != 0) throw std::runtime_error("Resize failed");
    map_addr = (char*)mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (map_addr == MAP_FAILED) throw std::runtime_error("Mmap failed");
    header = (FileHeader*)map_addr;
    uint32_t num_shards = 64; 
    shard_mask = num_shards - 1;
    if (is_new) {
        header->magic = PKV_MAGIC;
        header->total_size = size;
        header->num_shards = num_shards;
        header->version = 1;
    } else {
        if (header->magic != PKV_MAGIC) throw std::runtime_error("Corrupted DB or wrong format");
        num_shards = header->num_shards;
        shard_mask = num_shards - 1;
    }
    size_t index_start_offset = sizeof(FileHeader);
    size_t total_index_bytes = (size * 0.10); 
    size_t bytes_per_shard_index = total_index_bytes / num_shards;
    size_t items_per_shard = bytes_per_shard_index / sizeof(IndexEntry);
    size_t data_start_global = index_start_offset + (bytes_per_shard_index * num_shards);
    size_t data_bytes_per_shard = (size - data_start_global) / num_shards;
    shards.resize(num_shards);
    for (uint32_t i = 0; i < num_shards; ++i) {
        shards[i].index_table = (IndexEntry*)(map_addr + index_start_offset + (i * bytes_per_shard_index));
        shards[i].index_cap = items_per_shard;
        uint64_t my_data_start = data_start_global + (i * data_bytes_per_shard);
        if (is_new) {
             memset(shards[i].index_table, 0, items_per_shard * sizeof(IndexEntry));
             shards[i].write_cursor = my_data_start;
        } else {
            shards[i].write_cursor = my_data_start; 
        }
    }
}

PowerKV::~PowerKV() {
    munmap(map_addr, total_size);
    close(fd);
}

PowerKV::Shard& PowerKV::GetShard(uint64_t hash) {
    return shards[hash & shard_mask];
}

bool PowerKV::Set(std::string_view key, std::string_view val, uint64_t ttl_sec) {
    uint64_t h = Hash(key.data(), key.size());
    Shard& shard = GetShard(h);
    std::unique_lock<std::shared_mutex> lock(shard.mutex);
    uint64_t needed = key.size() + val.size() + sizeof(uint64_t);
    uint64_t write_pos = shard.write_cursor.load(std::memory_order_relaxed);
    if (write_pos + needed >= total_size) return false;
    char* dest = map_addr + write_pos;
    memcpy(dest, key.data(), key.size());
    memcpy(dest + key.size(), val.data(), val.size());
    shard.write_cursor.store(write_pos + needed, std::memory_order_relaxed);
    IndexEntry* table = shard.index_table;
    uint64_t cap = shard.index_cap;
    uint64_t idx = h % cap;

    for (uint64_t i = 0; i < cap; ++i) {
        uint64_t curr = (idx + i) % cap;
        if (table[curr].offset == 0 || table[curr].key_hash == h) {
            table[curr].key_hash = h;
            table[curr].offset = write_pos;
            table[curr].key_len = key.size();
            table[curr].val_len = val.size();
            table[curr].expiry = (ttl_sec > 0) ? (time(NULL) + ttl_sec) : 0;
            return true;
        }
    }
    
    return false; // Index Full
}

std::optional<std::string_view> PowerKV::Get(std::string_view key) {
    uint64_t h = Hash(key.data(), key.size());
    Shard& shard = GetShard(h);
    std::shared_lock<std::shared_mutex> lock(shard.mutex);
    IndexEntry* table = shard.index_table;
    uint64_t cap = shard.index_cap;
    uint64_t idx = h % cap;

    for (uint64_t i = 0; i < cap; ++i) {
        uint64_t curr = (idx + i) % cap;
        if (table[curr].offset == 0) return std::nullopt;

        if (table[curr].key_hash == h) {
            if (table[curr].expiry > 0 && table[curr].expiry < (uint64_t)time(NULL)) {
                return std::nullopt;
            }
            char* ptr = map_addr + table[curr].offset;
            if (table[curr].key_len == key.size() && 
                memcmp(ptr, key.data(), key.size()) == 0) {
                return std::string_view(ptr + table[curr].key_len, table[curr].val_len);
            }
        }
    }
    return std::nullopt;
}

bool PowerKV::Del(std::string_view key) {
    uint64_t h = Hash(key.data(), key.size());
    Shard& shard = GetShard(h);
    std::unique_lock<std::shared_mutex> lock(shard.mutex);
    IndexEntry* table = shard.index_table;
    uint64_t cap = shard.index_cap;
    uint64_t idx = h % cap;

    for (uint64_t i = 0; i < cap; ++i) {
        uint64_t curr = (idx + i) % cap;
        if (table[curr].offset == 0) return false;

        if (table[curr].key_hash == h) {
             table[curr].expiry = 1; 
             return true;
        }
    }
    return false;
}