import time
import unittest
from ikv_py.native_reader import NativeReader
from ikv_py.clientoptions import ClientOptions, ClientOptionsBuilder
import ikv_py.client as ikv_client
import ikv_py.document
from ikv_py.factory import create_new_reader, create_new_writer
from env_var import EnvReader

class TestInteg(unittest.TestCase):
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
        self.reader: ikv_client.IKVReader = create_new_reader(client_options)
        self.writer: ikv_client.IKVWriter = create_new_writer(client_options)
        self.reader.startup()
        self.writer.startup()

    @classmethod
    def tearDownClass(self):
        self.reader.shutdown()
        self.writer.shutdown()

    def test_single_doc(self):
        # upsert {"userid": "0", "firstname": "Alice"}
        document = ikv_py.document.IKVDocumentBuilder().put_string_field("userid", "0")\
            .put_string_field("firstname", "Alice").build()
        self.writer.upsert_fields(document)
        time.sleep(5)

        # read valid fields
        self.assertEqual("0", self.reader.get_string_value("0", "userid"))
        self.assertEqual("Alice", self.reader.get_string_value("0", "firstname"))

        # read valid fields for invalid document
        self.assertIsNone(self.reader.get_string_value("1", "userid"))

        # read invalid fields for valid document
        self.assertIsNone(self.reader.get_string_value("0", "foo_field"))
        

    def test_upsert_delete_doc(self):
        # upsert {"userid": "0", "firstname": "Alice"}
        document = ikv_py.document.IKVDocumentBuilder().put_string_field("userid", "0")\
            .put_string_field("firstname", "Alice").build()
        self.writer.upsert_fields(document)
       
        # delete {"userid": "0"}
        document = ikv_py.document.IKVDocumentBuilder().put_string_field("userid", "0").build()
        self.writer.delete_document(document)

        time.sleep(5)

        # read assertions
        self.assertIsNone(self.reader.get_string_value("0", "userid"))
        self.assertIsNone(self.reader.get_string_value("0", "firstname"))

    def test_upsert_drop_fields(self):
        # upsert {"userid": "0", "firstname": "Alice"}
        # upsert {"userid": "1", "firstname": "Bob", "city": "NYC"}
        # upsert {"userid": "2", "firstname": "Allison", "city": "NYC"}
        document = ikv_py.document.IKVDocumentBuilder().put_string_field("userid", "0")\
            .put_string_field("firstname", "Alice").build()
        self.writer.upsert_fields(document)
        
        document = ikv_py.document.IKVDocumentBuilder().put_string_field("userid", "1")\
            .put_string_field("firstname", "Bob").put_string_field("city", "NYC").build()
        self.writer.upsert_fields(document)
        
        document = ikv_py.document.IKVDocumentBuilder().put_string_field("userid", "2")\
            .put_string_field("firstname", "Allison").put_string_field("city", "NYC").build()
        self.writer.upsert_fields(document)

        # drop column for "firstname" and "city"
        self.writer.drop_fields_by_name(["firstname", "foo"])
        self.writer.drop_fields_by_name_prefix(["ci", "ciii"])

        time.sleep(5)

        # read assertions
        self.assertEqual("0", self.reader.get_string_value("0", "userid"))
        self.assertEqual("1", self.reader.get_string_value("1", "userid"))
        self.assertEqual("2", self.reader.get_string_value("2", "userid"))
        
        self.assertIsNone(self.reader.get_string_value("0", "firstname"))
        self.assertIsNone(self.reader.get_string_value("1", "firstname"))
        self.assertIsNone(self.reader.get_string_value("2", "firstname"))
        
        self.assertIsNone(self.reader.get_string_value("0", "city"))
        self.assertIsNone(self.reader.get_string_value("1", "city"))
        self.assertIsNone(self.reader.get_string_value("2", "city"))

# run: $> python3 tests/test_native_reader.py
#      $> [ikv-python-client/tests]$ python3 -m unittest
if __name__ == '__main__':
    unittest.main()