#!/usr/bin/env bash

dir=$(dirname "$0")
today=$(date "+%F")
current_year=$(date "+%Y")

racket -y ${dir}/dividends-extract.rkt -p "$1" -k "$2"
racket -y ${dir}/dividends-transform-load.rkt -p "$1"

7zr a /var/local/polygon/dividends/${current_year}.7z /var/local/polygon/dividends/${today}

racket -y ${dir}/dump-dolt-dividends.rkt -p "$1"
