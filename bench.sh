#!/bin/bash

bin/cr8 timeit -r 10 --warmup 10 "$1" "$2" | jq 'del(.client_runtimes) | del(.server_runtimes)' | bin/cr8 json2insert benchmarks "$3"
