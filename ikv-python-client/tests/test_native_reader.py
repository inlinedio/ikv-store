import unittest
from src.ikvpy.native_reader import NativeReader

class TestNativeReader(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.native_reader = \
            NativeReader("/Users/pushkar/projects/ikv-store/ikv/target/release/libikv.dylib")

    def test_health_check(self):
        self.assertTrue(self.native_reader.health_check("foo"))

# run: $> python3 tests/test_native_reader.py
#      $> [ikv-python-client/tests]$ python3 -m unittest
if __name__ == '__main__':
    unittest.main()