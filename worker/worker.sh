#!/bin/bash

killall python

date >> worker.out
python worker.py reala.ece.ubc.ca >> worker.out 2>> worker.out
