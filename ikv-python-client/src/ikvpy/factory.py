from ikvpy.clientoptions import ClientOptions
from ikvpy.reader import IKVReaderImpl
from ikvpy.writer import IKVWriterImpl
from ikvpy.client import IKVReader, IKVWriter

"""
Factory for creating concrete IKVReader and IKVWriter instances.
"""

def create_new_reader(client_options: ClientOptions) -> IKVReader:
    """
    Create new reader client by supplying configuration.
    args:
        client_options: reader configuration, see clientoptions.py to instantiate
    """
    return IKVReaderImpl(client_options)

def create_new_writer(client_options: ClientOptions) -> IKVWriter:
    """
    Create new writer client by supplying configuration.
    args:
        client_options: writer configuration, see clientoptions.py to instantiate
    """
    return IKVWriterImpl(client_options)