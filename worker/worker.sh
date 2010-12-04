#!/bin/bash

killall python
python worker.py > worker.out 2> worker.out
