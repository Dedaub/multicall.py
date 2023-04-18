import cProfile
import json
import os
import sys
import time
from typing import Mapping

import memray
from web3 import Web3
from web3.providers import HTTPProvider

from multicall import Call, Multicall


def read_conf() -> Mapping:
    with open(os.path.join(os.path.dirname(__file__), "conf.json"), "r") as fl:
        conf = json.load(fl)
    conf["networks"] = dict(
        zip(map(int, conf["networks"].keys()), conf["networks"].values())
    )
    return conf


def build_multicall(conf: Mapping, json_file: str) -> Multicall:
    with open(json_file, "r") as fl:
        data = json.load(fl)
        calls = [
            Call(
                target,
                [func, *args] if len(args) else func,
                [[(target, func), None]],
            )
            for target, func, *args in data["calls"]
        ]

        chain_id = data["chain_id"]
        require_success = data["require_success"]
        data["workers"]

        return Multicall(
            calls,
            max_conns=200,
            require_success=require_success,
            _w3=Web3(HTTPProvider(conf["networks"][chain_id])),
        )


def profile_multicall(multicall: Multicall, outfile: str):
    cProfile.run("multicall()", outfile)


def memory_multicall(multicall: Multicall, outfile: str):
    with memray.Tracker(outfile):
        multicall()


if __name__ == "__main__":

    conf = read_conf()

    for i, fname in enumerate(sys.argv[1:]):
        multicall = build_multicall(conf, fname)

        if conf.get("profile_runtime", True):
            print(f"Profiling runtime of {multicall}..")
            profile_multicall(multicall, f"multicall_{i}_{int(time.time())}")

        if conf.get("profile_memory", True):
            print(f"Profiling memory usage of {multicall}..")
            memory_multicall(multicall, f"multicall_{i}_{int(time.time())}.bin")
