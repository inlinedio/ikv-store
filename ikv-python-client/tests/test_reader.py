import unittest
from ikv_py.native_reader import NativeReader
from ikv_py.clientoptions import ClientOptions, ClientOptionsBuilder
import ikv_py.client as ikv_client
from ikv_py.factory import create_new_reader

class TestIKVReaderImpl(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        # setup client options
        client_options: ClientOptions = ClientOptionsBuilder() \
            .with_account_id("foo")\
            .with_account_passkey("bar")\
            .with_store_name("baz")\
            .with_mount_directory("/tmp/python-integ-test")\
            .build()
        
        # setup reader
        self.reader: ikv_client.IKVReader = create_new_reader(client_options)

    def test_startup(self):
        self.reader.startup()
    
    def test_shutdown(self):
        self.reader.shutdown()

# run: $> python3 tests/test_native_reader.py
#      $> [ikv-python-client/tests]$ python3 -m unittest
if __name__ == '__main__':
    unittest.main()