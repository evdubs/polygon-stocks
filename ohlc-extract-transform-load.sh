#!/usr/bin/env bash

dir=$(dirname "$0")
today=$(date "+%F")
today_year=$(date "+%Y")

racket -y ${dir}/ohlc-extract.rkt -d ${today} -k "$2"
racket -y ${dir}/ohlc-transform-load.rkt -d ${today} -p "$1"

7zr a /var/tmp/polygon/ohlc/${today_year}.7z /var/tmp/polygon/ohlc/${today}.json

racket -y ${dir}/dump-dolt-ohlcv.rkt -p "$1"
