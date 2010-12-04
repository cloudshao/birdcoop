#!/bin/bash

killall python

date >> worker.out
python worker.py >> worker.out 2>> worker.out
