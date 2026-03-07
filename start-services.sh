#!/bin/bash

echo "Starting Apache Atlas..."

cd /opt/apache-atlas/bin
./atlas_start.py

echo "Starting Context Engine..."

java -jar /opt/context/context-engine.jar --server.port=22000