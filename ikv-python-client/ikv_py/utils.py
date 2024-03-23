from typing import Tuple, Optional


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