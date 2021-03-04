#!/bin/sh

pipenv run spark-submit --driver-memory 16G --master="local[*]" ./generate.py