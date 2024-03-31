import unittest
from src.ikvpy.native_reader import NativeReader
from src.ikvpy.clientoptions import ClientOptions, ClientOptionsBuilder
import src.ikvpy.client as ikv_client
from src.ikvpy.factory import create_new_reader
from env_var import EnvReader

class TestIKVReaderImpl(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        # setup client options
        env = EnvReader()
        client_options: ClientOptions = ClientOptionsBuilder() \
            .with_account_id(env.get("ACCOUNT_ID"))\
            .with_account_passkey(env.get("ACCOUNT_PASSKEY"))\
            .with_store_name(env.get("STORE_NAME"))\
            .with_mount_directory(env.get("MOUNT_DIR"))\
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