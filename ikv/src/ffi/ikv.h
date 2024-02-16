#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

typedef struct BytesBuffer {
  int32_t length;
  uint8_t *start;
} BytesBuffer;

void hello_world(const char *input);

int64_t open_index(const char *config, int32_t config_len);

void close_index(int64_t handle);

struct BytesBuffer get_field_value(int64_t handle,
                                   const char *pkey,
                                   int32_t pkey_len,
                                   const char *field_name);

// no need to call if BytesBuffer.start is null
void free_bytes_buffer(struct BytesBuffer buf);
