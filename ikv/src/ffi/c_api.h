struct BytesBuffer {
  uint8_t *data;
  uintptr_t len;
};

void hello_world(const char *name);

// int64_t open(struct BytesBuffer config);

void close(int64_t handle);

struct BytesBuffer read_field(int64_t handle, struct BytesBuffer primary_key, const char *field_name);
