from collections.abc import Callable, Iterable
from typing import Any, Union

import eth_retry
from eth_typing import Address, ChecksumAddress, HexAddress
from eth_typing.abi import Decodable
from eth_utils import to_checksum_address
from web3 import Web3

from multicall.loggers import setup_logger
from multicall.signature import Signature

logger = setup_logger(__name__)

AnyAddress = Union[str, Address, ChecksumAddress, HexAddress]


class Call:
    def __init__(
        self,
        target: AnyAddress,
        function: (
            str | list[str | Any]
        ),  # 'funcName(dtype)(dtype)' or ['funcName(dtype)(dtype)', input0, input1, ...]
        returns: Iterable[tuple[str, Callable | None]] | None = None,
        block_id: int | None = None,
        gas_limit: int | None = None,
        state_override_code: str | None = None,
        _w3: Web3 | None = None,
    ) -> None:
        self.target = to_checksum_address(target)
        self.returns = returns
        self.block_id = block_id
        self.gas_limit = gas_limit
        self.state_override_code = state_override_code
        self.w3 = _w3

        self.args: list[Any] | None
        if isinstance(function, list):
            func, *self.args = function
            self.function: str = func
        else:
            self.function = function
            self.args = None

        self.signature = Signature(self.function)

    def __repr__(self) -> str:
        return f"Call {self.target}.{self.function}"

    @property
    def data(self) -> bytes:
        return self.signature.encode_data(self.args)

    @staticmethod
    def decode_output(
        output: Decodable,
        signature: Signature,
        returns: Iterable[tuple[str, Callable | None]] | None = None,
        success: bool | None = None,
    ) -> Any:

        if success is None:

            def apply_handler(handler, value):
                return handler(value)

        else:

            def apply_handler(handler, value):
                return handler(success, value)

        if success is None or success:
            try:
                decoded = signature.decode_data(output)
            except Exception:
                success, decoded = False, [None] * (1 if not returns else len(returns))  # type: ignore
        else:
            decoded = [None] * (1 if not returns else len(returns))  # type: ignore

        logger.debug(f"returns: {returns}")
        logger.debug(f"decoded: {decoded}")

        if returns:
            return {
                name: apply_handler(handler, value) if handler is not None else value
                for (name, handler), value in zip(returns, decoded)
            }
        else:
            return decoded if len(decoded) > 1 else decoded[0]

    @eth_retry.auto_retry
    def __call__(self, args: Any | None = None) -> Any:
        if self.w3 is None:
            raise RuntimeError
        args = Call.prep_args(
            self.target,
            self.signature,
            args or self.args,
            self.block_id,
            self.gas_limit,
            self.state_override_code,
        )
        return Call.decode_output(
            self.w3.eth.call(*args),
            self.signature,
            self.returns,
        )

    @staticmethod
    def prep_args(
        target: str,
        signature: Signature,
        args: Any | None,
        block_id: int | None,
        gas_limit: int | None,
        state_override_code: str | None,
    ) -> list:

        calldata = signature.encode_data(args)

        params: dict[str, Any] = {"to": target, "data": calldata}
        if gas_limit:
            params["gas"] = gas_limit

        args = [params, block_id]
        if state_override_code:
            args.append({target: {"code": state_override_code}})

        return args
