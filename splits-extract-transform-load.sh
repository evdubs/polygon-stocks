#!/usr/bin/env bash

dir=$(dirname "$0")
today=$(date "+%F")
current_year=$(date "+%Y")

racket -y ${dir}/splits-extract.rkt -p "$1" -k "$2"
racket -y ${dir}/splits-transform-load.rkt -p "$1"

7zr a /var/tmp/polygon/splits/${current_year}.7z /var/tmp/polygon/splits/${today}

racket -y ${dir}/dump-dolt-splits.rkt -p "$1"
