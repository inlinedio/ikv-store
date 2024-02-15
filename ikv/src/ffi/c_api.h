/*struct BytesBuffer {
  uint8_t *data;
  uintptr_t len;
};*/

extern "C" {
void hello_world(const char *input);

long open(const unsigned char *config);

void close(long handle);
}

// struct BytesBuffer read_field(int64_t handle, struct BytesBuffer primary_key, const char *field_name);
