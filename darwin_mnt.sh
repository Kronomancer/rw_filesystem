#!/bin/sh
MNT=./mount
mkdir -p $MNT
./cpe453fs -olocal -s -d $MNT debug.fs
