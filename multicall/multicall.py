import asyncio
import itertools
import json
import multiprocessing
import multiprocessing.pool
from collections import ChainMap
from collections.abc import Sequence
from time import time
from typing import Any, Union, cast

import aiohttp
from web3 import Web3
from web3.providers import HTTPProvider

from multicall.call import Call
from multicall.constants import (
    GAS_LIMIT,
    MULTICALL2_ADDRESSES,
    MULTICALL3_ADDRESSES,
    MULTICALL3_BYTECODE,
)
from multicall.errors import EthRPCError
from multicall.loggers import setup_logger
from multicall.utils import chain_id, get_endpoint, state_override_supported

logger = setup_logger(__name__)

CallResponse = tuple[Union[None, bool], bytes]


def get_args(
    calls: list[Call], require_success: bool = True
) -> list[bool | list[list[Any]]]:
    if require_success is True:
        return [[[call.target, call.data] for call in calls]]
    return [require_success, [[call.target, call.data] for call in calls]]


def unpack_aggregate_outputs(outputs: Any) -> tuple[CallResponse, ...]:
    return tuple((None, output) for output in outputs)


def unpack_batch_results(batch_results: list[list[CallResponse]]) -> list[CallResponse]:
    return [result for batch in batch_results for result in batch]


def _initialize_multiprocessing():
    if multiprocessing.get_start_method(allow_none=True) is None:
        start_methods = multiprocessing.get_all_start_methods()
        if "forkserver" in start_methods:
            multiprocessing.set_start_method("forkserver")
        elif "spawn" in start_methods:
            multiprocessing.set_start_method("spawn")

    if multiprocessing.get_start_method(allow_none=False) == "fork":
        logger.warning(
            "Using fork as multiprocessing start_method, memory usage may be high."
        )


class Multicall:
    def __init__(
        self,
        calls: list[Call],
        _w3: Web3,
        block_id: int | None = None,
        require_success: bool = True,
        gas_limit: int = GAS_LIMIT,
        # Fork Specific Args
        batch_size: int | None = None,
        retries: int = 3,
        max_conns: int = 20,
        max_workers: int = min(12, multiprocessing.cpu_count() - 1),
        parallel_threshold: int = 1,  # when the number of function calls to execute is above this threshold, multiprocessing is used
        batch_timeout: int = 300,  # timeout in seconds for a multicall batch
    ) -> None:

        self.calls = calls
        self.block_id = block_id
        self.require_success = require_success
        self.gas_limit = gas_limit
        self.chainid = chain_id(_w3)

        # NOTE: do not assign the _w3 object, it cant be pickled for multiprocessing

        if require_success is True:
            multicall_map = (
                MULTICALL3_ADDRESSES
                if self.chainid in MULTICALL3_ADDRESSES
                else MULTICALL2_ADDRESSES
            )
            self.multicall_sig = "aggregate((address,bytes)[])(uint256,bytes[])"
        else:
            multicall_map = (
                MULTICALL3_ADDRESSES
                if self.chainid in MULTICALL3_ADDRESSES
                else MULTICALL2_ADDRESSES
            )
            self.multicall_sig = "tryBlockAndAggregate(bool,(address,bytes)[])(uint256,uint256,(bool,bytes)[])"
        self.multicall_address = multicall_map[self.chainid]

        self.batch_size = batch_size or -(-len(calls) // max_conns)
        self.retries = retries
        self.max_conns = max_conns
        self.max_workers = max_workers

        self.parallel_threshold = parallel_threshold if max_workers > 1 else 1 << 31
        self.batch_timeout = batch_timeout

        self.node_uri = get_endpoint(_w3)

    def __repr__(self) -> str:
        return f'Multicall {", ".join({call.function for call in self.calls})}, {len(self.calls)} calls'

    def __call__(self) -> dict[str, Any]:
        if len(self.calls) == 0:
            return {}

        start = time()
        response: dict[str, Any]
        if -(-len(self.calls) // self.batch_size) > self.parallel_threshold:
            with multiprocessing.Pool(processes=self.max_workers) as p:
                response = self.fetch_outputs(p)
        else:
            response = self.fetch_outputs()
        logger.debug(f"Multicall took {time() - start}s")
        return response

    def encode_args(self, calls_batch: list[Call]) -> list[Any]:
        _args = get_args(calls_batch, self.require_success)
        calldata = f"0x{self.aggregate.signature.encode_data(_args).hex()}"

        params: dict[str, Any] = {"to": self.aggregate.target, "data": calldata}
        if self.gas_limit:
            params["gas"] = f"0x{self.gas_limit:x}"

        args = [
            params,
            self.block_id if self.block_id is not None else "latest",
        ]

        return args

    def decode_outputs(self, calls_batch: list[Call], result: bytes):
        if self.require_success is True:
            _, outputs = Call.decode_output(
                result, self.aggregate.signature, self.aggregate.returns
            )
            outputs = unpack_aggregate_outputs(outputs)
        else:
            _, _, outputs = Call.decode_output(
                result, self.aggregate.signature, self.aggregate.returns
            )

        outputs = [
            Call.decode_output(output, call.signature, call.returns, success)
            for call, (success, output) in zip(calls_batch, outputs)
        ]
        return {name: result for output in outputs for name, result in output.items()}

    async def rpc_eth_call(self, session: aiohttp.ClientSession, args):

        async with session.post(
            self.node_uri,
            headers={"Content-Type": "application/json"},
            data=json.dumps(
                {
                    "params": args,
                    "method": "eth_call",
                    "id": 1,
                    "jsonrpc": "2.0",
                }
            ),
        ) as response:

            assert response.status == 200, RuntimeError(f"Network Error: {response}")
            data = await response.json()
            if "error" in data:
                if "out of gas" in data["error"]["message"]:
                    return EthRPCError.OUT_OF_GAS
                elif "execution reverted" in data["error"]["message"]:
                    return EthRPCError.EXECUTION_REVERTED
                else:
                    return EthRPCError.UNKNOWN
            return bytes.fromhex(data["result"][2:])

    async def rpc_aggregator(self, args_list: list[list]) -> list[EthRPCError | bytes]:

        async with aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=self.max_conns),
            timeout=aiohttp.ClientTimeout(self.batch_timeout),
        ) as session:
            return await asyncio.gather(
                *[self.rpc_eth_call(session, args) for args in args_list]
            )

    def fetch_outputs(
        self, p: multiprocessing.pool.Pool | None = None
    ) -> dict[str, Any]:
        calls = self.calls

        outputs: dict[str, Any] = {}

        for batch_size in itertools.chain(
            (self.batch_size // (1 << i) for i in range(self.retries)), [1]
        ):

            if len(calls) == 0:
                break

            batches = [
                calls[batch : batch + batch_size]
                for batch in range(0, len(calls), batch_size)
            ]

            encoded_args: list
            if p and len(batches) > self.parallel_threshold:
                encoded_args = list(
                    p.imap(
                        self.encode_args,
                        batches,
                        chunksize=-(-len(batches) // self.max_workers),
                    )
                )
            else:
                encoded_args = list(map(self.encode_args, batches))

            results = asyncio.run(self.rpc_aggregator(encoded_args))

            if self.require_success and EthRPCError.EXECUTION_REVERTED in results:
                raise RuntimeError("Multicall with require_success=True failed.")

            # find remaining calls
            calls = list(
                itertools.chain(
                    *[
                        batches[i]
                        for i, x in enumerate(results)
                        if x == EthRPCError.OUT_OF_GAS
                    ]
                )
            )

            successes = [
                (batch, result)
                for batch, result in zip(batches, results)
                if not isinstance(result, EthRPCError)
            ]

            batches, results = zip(*successes) if len(successes) else ([], [])  # type: ignore
            batches = cast(Sequence[list[Call]], batches)  # type: ignore
            results = cast(Sequence[bytes], results)  # type: ignore

            if p and len(batches) > self.parallel_threshold:
                outputs.update(
                    ChainMap(
                        *p.starmap(
                            self.decode_outputs,
                            zip(batches, results),
                            chunksize=-(-len(batches) // self.max_workers),
                        )
                    )
                )
            else:
                outputs.update(ChainMap(*map(self.decode_outputs, batches, results)))  # type: ignore

        return outputs

    @property
    def aggregate(self) -> Call:

        if state_override_supported(self.chainid):
            return Call(
                self.multicall_address,
                self.multicall_sig,
                returns=None,
                _w3=Web3(HTTPProvider(self.node_uri)),
                block_id=self.block_id,
                gas_limit=self.gas_limit,
                state_override_code=MULTICALL3_BYTECODE,
            )

        # If state override is not supported, we simply skip it.
        # This will mean you're unable to access full historical data on chains without state override support.
        return Call(
            self.multicall_address,
            self.multicall_sig,
            returns=None,
            block_id=self.block_id,
            _w3=Web3(HTTPProvider(self.node_uri)),
            gas_limit=self.gas_limit,
        )
