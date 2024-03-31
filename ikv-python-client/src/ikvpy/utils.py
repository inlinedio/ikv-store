import struct

from typing import List, Tuple, Optional, Iterator

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

def _parse_semver(version) -> Tuple[int, int, int, Optional[str]]:
    version_parts = version.split('.')
    major, minor, patch = map(int, version_parts[:3])
    pre_release = version_parts[3] if len(version_parts) > 3 and '-' in version_parts[3] else None
    return major, minor, patch, pre_release

def compare_semver(version1, version2):
    """
    Compare two semver strings.
    result > 0 iff v1 > v2
    result < 0 iff v1 < v2
    result == 0 iff v1 == v2
    """
    v1 = _parse_semver(version1)
    v2 = _parse_semver(version2)
    return (v1 > v2) - (v1 < v2)

def concat_strings_with_size_prefix(input: List[str] = []) -> bytearray:
    if not len(input):
        return bytearray()

    capacity = 0
    input_as_bytes = [None] * len(input)
    for i, inp in enumerate(input):
        inp_bytes = inp.encode("utf-8")
        capacity += 4 + len(inp_bytes)
        input_as_bytes[i] = inp_bytes

    output = bytearray(capacity)

    i = 0
    for inp in input_as_bytes:
        # lower endian i32
        struct.pack_into('<i', output, i, len(inp))
        i += 4
        # utf8 bytes
        output[i:i+len(inp)] = inp
        i += len(inp)

    return output

def concat_as_utf8_with_size_prefix(bytes_input: List[bytes] = [], str_input: List[str] = []) -> bytearray:
    if not len(bytes_input) and not len(str_input):
        return bytearray()

    capacity = 0
    for inp in bytes_input:
        capacity += 4 + len(inp)

    str_input_as_bytes = [None] * len(str_input)
    for i, inp in enumerate(str_input):
        inp_bytes = inp.encode("utf-8")
        capacity += 4 + len(inp_bytes)
        str_input_as_bytes[i] = inp_bytes

    output = bytearray(capacity)

    i = 0
    for inp_list in [bytes_input, str_input_as_bytes]:
        for inp in inp_list:
            # lower endian i32
            struct.pack_into('<i', output, i, len(inp))
            i += 4
            # utf8 bytes
            output[i:i+len(inp)] = inp
            i += len(inp)

    return output

def unpack_size_prefixed_bytes_as_bytes(input: bytes) -> Iterator[Optional[bytes]]:
    i = 0
    while i < len(input):
        size = struct.unpack_from('<i', input, i)[0]
        i += 4

        if size == -1:
            yield None
        elif size == 0:
            yield bytes()
        else:
            yield input[i:i+size]
            i += size

def unpack_size_prefixed_bytes_as_str(input: bytes) -> Iterator[Optional[str]]:
    i = 0
    while i < len(input):
        size = struct.unpack_from('<i', input, i)[0]
        i += 4

        if size == -1:
            yield None
        elif size == 0:
            yield ""
        else:
            yield str(memoryview(input[i:i+size]), 'utf-8')
            i += size

