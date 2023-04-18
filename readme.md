# multicall.py

python interface for makerdao's [multicall](https://github.com/makerdao/multicall) and a port of [multicall.js](https://github.com/makerdao/multicall.js)

## installation

```
pip install multicall
```

## Differences from `banteg/multicall`

- Some interface changes; see below.
- Removed default `Web3` instance.
- Batch sizes and number of workers specified by user; removed `NotSoBrightBatcher`.
- Multicalls are much more configurable.
- A fallback to individual `eth_call`s if multicall keeps failing with out of gas errors.
  This provides handling for calls that `throw()`.

- The package still uses asyncio+multiprocessing, but the workflow has changed:

  1. Calls are split into batches of `batch_size`.
  2. Encoding is done in a (per `Multicall` object) process pool (depending on the value of `parallel_threshold`).
  3. The web3 call is done in an asyncio loop in the main process.
  4. Decoding is done in a process pool (depending on the value of `parallel_threshold`).
  5. The calls in failed batches are rebatched and rerun for `retries` times.

  By default the multiprocessing `forkserver` [start method](https://docs.python.org/3/library/multiprocessing.html#contexts-and-start-methods) is used to avoid high memory consumption.

## example

```python
from brownie import web3
from multicall import Call, Multicall

# assuming you are on kovan
MKR_TOKEN = '0xaaf64bfcc32d0f15873a02163e7e500671a4ffcd'
MKR_WHALE = '0xdb33dfd3d61308c33c63209845dad3e6bfb2c674'
MKR_FISH = '0x2dfcedcb401557354d0cf174876ab17bfd6f4efd'

def from_wei(value):
    return value / 1e18

multi = Multicall([
    Call(MKR_TOKEN, ['balanceOf(address)(uint256)', MKR_WHALE], [('whale', from_wei)]),
    Call(MKR_TOKEN, ['balanceOf(address)(uint256)', MKR_FISH], [('fish', from_wei)]),
    Call(MKR_TOKEN, 'totalSupply()(uint256)', [('supply', from_wei)]),
])

multi()  # {'whale': 566437.0921992733, 'fish': 7005.0, 'supply': 1000003.1220798912}

# seth-style calls
Call(MKR_TOKEN, ['balanceOf(address)(uint256)', MKR_WHALE], _w3=web3.web)()
Call(MKR_TOKEN, 'balanceOf(address)(uint256)', _w3=web3.web)(MKR_WHALE)
# return values processing
Call(MKR_TOKEN, 'totalSupply()(uint256)', [['supply', from_wei]], _w3=web3.web)()
```

## api

### `Signature(signature)`

- `signature` is a seth-style function signature of `function_name(input,types)(output,types)`. it also supports structs which need to be broken down to basic parts, e.g. `(address,bytes)[]`.

use `encode_data(args)` with input args to get the calldata. use `decode_data(output)` with the output to decode the result.

### `Call(target, function, returns=None, block_id=None, gas_limit=None, _w3=None)`

- `target` is the `to` address which is supplied to `eth_call`.
- `function` can be either seth-style signature of `method(input,types)(output,types)` or a list of `[signature, *args]`.
- `returns` is a list of `[name, handler]` for return values. if `returns` argument is omitted, you get a tuple, otherwise you get a dict. to skip processing of a value, pass `None` as a handler.
- `_w3` is a `Web3` instance to be used for the `eth_call`. If `None`, `Call(...)()` will raise a `RuntimeError`.

use `Call(...)()` with predefined args or `Call(...)(args)` to reuse a prepared call with different args.

use `decode_output(output)` with to decode the output and process it with `returns` handlers.

### `Multicall`

```
Multicall(
    calls: List[Call],
    batch_size: Optional[int] = None,
    block_id: Optional[int] = None,
    gas_limit: int = 1 << 31,
    retries: int = 3,
    require_success: bool = True,
    _w3: Optional[Web3] = None,
    max_conns: int = 20,
    max_workers: int = min(12, multiprocessing.cpu_count() - 1),
    parallel_threshold: int = 1,
    batch_timeout: int = 300,
)
```

- `calls` is a list of `Call`s with prepared values.
- `batch_size` is the size of batches to give to the multicall contract.
- `block_id` is the block number at which the multicall is executed.
- `gas_limit` is the amount of gas (in wei) allocated to the multicall.
- `retries` is the number of attempts to rerun failed calls.
- `require_success` determines whether or not all calls must be successful.
- `_w3` is a `Web3` instance whose uri is to be used for the multicall. If `None`, `Multicall(...)()` will raise a `RuntimeError`.
- `max_conns` is the maximum number of connections used for the multicall.
- `max_workers` is the number of processes used by the multicall encoding/decoding.
- `parallel_threshold` is the number of calls above which parallel encoding/decoding is activated.
- `batch_timeout` is the timeout for a single multicall batch.

use `Multicall(...)()` to get the result of a prepared multicall.

### Environment Variables

- MULTICALL_DEBUG: if set, sets logging level for all library loggers to logging.DEBUG

## Tests

Tests require a config file at `tests/conf.json`.

The config file must have the following format:

```json
{
    "networks": {
        "1": http://localhost,
        ...
    }
}
```

To test the package, make sure `pytest` is installed in your `venv`, then run the following command:

```
python -m pytest
```

Some of the test multicalls are JSON serialized and GZIP-compressed (under the directory `/tests/data`), so your mileage may vary dependening on OS.

- MULTICALL_PROCESSES: pass an integer > 1 to use multiprocessing for encoding args and decoding results. Default: 1, which executes all code in the main process.
- AIOHTTP_TIMEOUT: sets aiohttp timeout period in seconds for async calls to node. Default: 30

## test

```bash
export WEB3_INFURE_PROJECT_ID=<your_infura_id>
export PYTEST_NETWORK='mainnet'
poetry run python -m pytest
```
