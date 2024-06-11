#!/usr/bin/env bash
# #############################################################################
# The driver test package utilizes code from the test suite files from the
# go-shelve module.
#
# This script ensures the following files are kept up-to-date:
# - test/db_main.go
# - test/codec_main.go
# #############################################################################

# Copy the DB test suite file
cp ../sdb/v1/db_main_test.go ./test/db_main.go

# Copy the Codec test suite file
cp ../shelve/codec_main_test.go ./test/codec_main.go

# Update the package name
sed -i -e 's/package sdb/package shelvetest/g' ./test/db_main.go
sed -i -e 's/package shelve/package shelvetest/g' ./test/codec_main.go
