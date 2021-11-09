#!/usr/bin/env bash

rm -rf ./bin
mkdir ./bin

# golocation="/usr/local/go/bin/go"
golocation="go"
# golocation=$(which go) # for local
echo $golocation

$golocation build -o main ./src/main.go

touch stable_storage.json
ls
cp main ./bin/
cp run.sh ./bin/
cp stable_storage.json ./bin/
