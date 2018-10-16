TOP := $(dir $(lastword $(MAKEFILE_LIST)))
ROOT = $(realpath $(TOP))

test:
	go test -v .
	go test -v ./util
