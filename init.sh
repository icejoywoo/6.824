#!/bin/bash

CURRENT_DIR=$(cd $(dirname ${BASH_SOURCE[0]}); pwd)

export GOPATH=${CURRENT_DIR}:${GOPATH}
