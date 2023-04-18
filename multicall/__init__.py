from multicall.call import Call
from multicall.multicall import Multicall, _initialize_multiprocessing
from multicall.signature import Signature

# try to use forkserver/spawn
_initialize_multiprocessing()
