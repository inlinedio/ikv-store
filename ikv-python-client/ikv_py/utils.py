def is_valid_str_or_raise(input: str) -> str:
    if not isinstance(input, str):
        raise TypeError("expecting a string")
    if not input.strip():
        raise ValueError("expecting non-empty string")
    return input

def is_valid_bytes_or_raise(input: bytes) -> bytes:
    if not isinstance(input, bytes):
        raise TypeError("expecting bytes")
    if len(input) == 0:
        raise ValueError("expecting non-empty bytes")
    return input