#!/bin/bash

for file in /home/mayank/Documents/NYSE/*;
do
    echo "-----------------Processing file-----------------------------"
    echo $file
    echo "-----------------****************----------------------------"
    mongoimport --db nyse --collection nysedata --type csv --file $file --headerline
done

