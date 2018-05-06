#!/usr/bin/env bash

go get -t -v ./...

./go.test.sh

bash <(curl -s https://codecov.io/bash)
