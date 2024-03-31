import unittest
import src.ikvpy.utils as utils

class TestUtils(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        pass

    def test_roundtrip_strings(self):
        cases = [["foo", "bar", "", "bar"], [], [""]]
        
        # write as str, read as str
        for input in cases:        
            serialized = bytes(utils.concat_strings_with_size_prefix(input))
            deserialized = list(utils.unpack_size_prefixed_bytes_as_str(serialized))
            self.assertEqual(input, deserialized)
        
        # write as str, read as bytes
        for input in cases:        
            serialized = bytes(utils.concat_as_utf8_with_size_prefix([], input))
            deserialized = list(utils.unpack_size_prefixed_bytes_as_bytes(serialized))
            deserialized = [str(x, "utf-8") for x in deserialized]
            self.assertEqual(input, deserialized)

    def test_roundtrip_bytes(self):
        cases = [["foo", "bar", "", "bar"], [], [""]]
        
        # write as bytes, read as bytes
        for input in cases:
            input = [x.encode("utf-8") for x in input]     
            serialized = bytes(utils.concat_as_utf8_with_size_prefix(input, []))
            deserialized = list(utils.unpack_size_prefixed_bytes_as_bytes(serialized))
            self.assertEqual(input, deserialized)

    def test_roundtrip(self):
        input_bytes = ["bar".encode(), "".encode()]
        input_str = ["", "foo"]

        serialized = bytes(utils.concat_as_utf8_with_size_prefix(input_bytes, input_str))
        deserialized = list(utils.unpack_size_prefixed_bytes_as_bytes(serialized))

        self.assertEqual(input_bytes[0], deserialized[0])
        self.assertEqual(input_bytes[1], deserialized[1])
        self.assertEqual(input_str[0], str(deserialized[2], "utf-8"))
        self.assertEqual(input_str[1], str(deserialized[3], "utf-8"))

# run: $> python3 tests/test_utils.py
#      $> [ikv-python-client/tests]$ python3 -m unittest
if __name__ == '__main__':
    unittest.main()