import os, sys
import asyncio
import functools
from enum import IntEnum

sys.path.append('./py_export/');
import py_export.CoreProtocol_pb2 as CoreProtocol_pb2
from py_export.CoreProtocol_pb2 import EmptyMessage, StringMessage

_reader, _writer = None, None

class DFHackReplyCode(IntEnum):
    RPC_REPLY_RESULT = -1
    RPC_REPLY_FAIL   = -2
    RPC_REPLY_TEXT   = -3
    RPC_REQUEST_QUIT = -4

def header(id, size):
    return id.to_bytes(2, sys.byteorder, signed=True) + \
        b'\x00\x00' + \
        size.to_bytes(4, sys.byteorder)

async def get_header():
    h = await _reader.read(8)
    id = int.from_bytes(h[0:2], sys.byteorder, signed=True)
    size = int.from_bytes(h[4:7], sys.byteorder)
    return id, size

def request(id, msg):
    s = msg.SerializeToString()
    h = header(id, len(s))
    return h + s

def unmarshal(id, msg):
    if   id == DFHackReplyCode.RPC_REPLY_RESULT:
        obj = CoreProtocol_pb2.CoreBindReply()
        obj.ParseFromString(msg)
        return obj.assigned_id
    elif id == DFHackReplyCode.RPC_REPLY_TEXT:
        obj = CoreProtocol_pb2.CoreTextNotification()
        obj.ParseFromString(msg)
        raise Exception(obj)
    # TODO others

@functools.lru_cache(maxsize=65534)
async def BindMethod(method, input_msg, output_msg, plugin=''):
    """Issue a CoreBindRequest to DFHack and caches the returned identifier number
    """
    br = CoreProtocol_pb2.CoreBindRequest()
    br.method, br.input_msg, br.output_msg, br.plugin = \
        method, input_msg.DESCRIPTOR.full_name, output_msg.DESCRIPTOR.full_name, plugin
    _writer.write( request(0, br) )
    h = await _reader.read(8)
    id, size = int.from_bytes(h[0:2], sys.byteorder, signed=True), int.from_bytes(h[4:7], sys.byteorder)
    return unmarshal(id, await _reader.read(size))

async def close():
    buf = header(DFHackReplyCode.RPC_REQUEST_QUIT, 0) + b'\x00\x00\x00\x00'
    _writer.write(buf) ; await _writer.drain()
    _writer.close() ; await _writer.wait_closed()

def handshake_request():
    n = 1
    return b'DFHack?\n' + n.to_bytes(4, sys.byteorder, signed=False)

async def connect():
    global _reader, _writer
    _reader, _writer = await asyncio.open_connection('127.0.0.1', 5000)
    _writer.write( handshake_request() )
    msg = await _reader.read(12)
    if b'DFHack!\n\x01\x00\x00\x00' != msg:  # handshake_reply
        _reader, _writer = None, None

def remote(plugin=''):
    """ Decorator that uses type annotations to bind DFHack Remote functions

    Example:
    @remote(plugin='RemoteFortressReader')
    async def GetVersionInfo(input: EmptyMessage = None, output: VersionInfo = None):

    @remote
    async def GetVersion(output: StringMessage = None): 
    """

    from functools import wraps, update_wrapper
    from inspect import signature
    input, output, function, _plugin = None, None, None, None

    async def wrapper(*args, **kwds):
        #if isinstance(plugin, str):
        #    kwds['plugin'] = plugin
        _id = await BindMethod(function.__name__, input, output, plugin=_plugin, **kwds)

        _writer.write( request(_id, input()) )

        id, size = await get_header()
        if id == DFHackReplyCode.RPC_REPLY_RESULT:
            buffer = await _reader.read(size)
            size -= len(buffer)
            while size:
                more = await _reader.read(size)
                buffer += more
                size -= len(more)
            obj = output()
            obj.ParseFromString(buffer)
            return obj

    def parse(f):
        nonlocal input, output, function
        function = f
        p = signature(f).parameters
        try:
            input = p['input'].annotation
        except KeyError:
            input = EmptyMessage
        output = p['output'].annotation
        return update_wrapper(wrapper, f)

    # For ease of writing signatures, let's use 'plugin' also for plain decorator
    if isinstance(plugin, str):  # The plugin name
        _plugin = plugin
        return parse
    else:
        _plugin = ''
        return parse(plugin)  # The decorated function

## Declare DFHack exported interfaces
@remote
async def GetVersion(output: StringMessage = None): pass 

from BasicApi_pb2 import GetWorldInfoOut
@remote
async def GetWorldInfo(output: GetWorldInfoOut = None): pass 

from RemoteFortressReader_pb2 import VersionInfo
@remote(plugin='RemoteFortressReader')
async def GetVersionInfo(output: VersionInfo = None): pass

from RemoteFortressReader_pb2 import UnitList
@remote(plugin='RemoteFortressReader')
async def GetUnitList(output: UnitList = None): pass

# Test
async def main():
    await connect()
    print( await GetVersionInfo() )
    print( "Units: ", len((await GetUnitList()).creature_list) )
    await close()

asyncio.run(main())
