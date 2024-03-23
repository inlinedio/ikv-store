from clientoptions import ClientOptions
from reader import IKVReaderImpl
from client import IKVReader, IKVWriter

"""
Factory methods
"""
def create_new_reader(client_options: ClientOptions) -> IKVReader:
    return IKVReaderImpl(client_options)

def create_new_writer(client_options: ClientOptions) -> IKVWriter:
    pass