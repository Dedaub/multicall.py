from collections.abc import Iterable
from typing import Any

from web3 import Web3

from multicall.constants import NO_STATE_OVERRIDE

chainids: dict[Web3, int] = {}


def chunks(lst: list, n: int):
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def chain_id(w3: Web3) -> int:
    """
    Returns chain id for an instance of Web3. Helps save repeat calls to node.
    """
    try:
        return chainids[w3]
    except KeyError:
        chainids[w3] = w3.eth.chain_id
        return chainids[w3]


def get_endpoint(w3: Web3) -> str:
    provider = w3.provider
    if isinstance(provider, str):
        return provider
    if hasattr(provider, "_active_provider"):
        provider = provider._get_active_provider(False)  # type: ignore
    return provider.endpoint_uri  # type: ignore


def raise_if_exception(obj: Any) -> None:
    if isinstance(obj, Exception):
        raise obj


def raise_if_exception_in(iterable: Iterable[Any]) -> None:
    for obj in iterable:
        raise_if_exception(obj)


def state_override_supported(chain_id: int) -> bool:
    if chain_id in NO_STATE_OVERRIDE:
        return False
    return True
