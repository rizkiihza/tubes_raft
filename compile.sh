#!/bin/bash

# compile
echo "Compiling..."
g++ -o bin/main -I src/ src/*.cpp --std=c++11 -g
#cp bin/main bin/jawaban_asisten
