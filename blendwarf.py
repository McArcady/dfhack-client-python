import asyncio
from dfhack_remote import remote, connect, close, StringMessage, EmptyMessage

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
