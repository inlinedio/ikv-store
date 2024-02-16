#include <stdint.h>

typedef struct BytesBuffer {
    int32_t length;
    char* start;
} BytesBuf;

void hello_world_a(const char *input);

int64_t open_index(const char *config, int32_t config_len);

void close_index(int64_t handle);

// Fetch a field value. Returns size prefixed byte array.
// caller must free the returned memory
BytesBuffer get_field_value(int64_t handle, const char *pkey, int32_t pkey_len, const char *field_name);

void free_bytes_buffer(BytesBuffer buf)