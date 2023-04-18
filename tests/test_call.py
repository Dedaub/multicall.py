from web3 import Web3
from web3.providers import HTTPProvider

from multicall import Call

from .settings import CONFIG

CHAI = "0x06AF07097C9Eeb7fD685c692751D5C66dB49c215"


def from_wei(value):
    return value / 1e18


w3 = {
    int(chain_id): Web3(HTTPProvider(network_uri))
    for chain_id, network_uri in CONFIG["networks"].items()
}


def test_call():
    call = Call(CHAI, "name()(string)", [["name", None]], _w3=w3[1])
    assert call() == {"name": "Chai"}


def test_call_with_args():
    call = Call(CHAI, "balanceOf(address)(uint256)", [["balance", from_wei]], _w3=w3[1])
    assert isinstance(call([CHAI])["balance"], float)


def test_call_with_predefined_args():
    call = Call(
        CHAI, ["balanceOf(address)(uint256)", CHAI], [["balance", from_wei]], _w3=w3[1]
    )
    assert isinstance(call()["balance"], float)
