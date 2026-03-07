#!/bin/bash

echo "Starting Apache Atlas..."

cd /opt/apache-atlas/bin
./atlas_start.py &
echo "Starting Atlas waiting for 10 seconds for process initialization..."
sleep 10

echo "Starting Context Engine..."

java -jar /opt/context/context-engine.jar --server.port=22000