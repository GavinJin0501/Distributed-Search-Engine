#!/bin/bash
# Invert index to create a mapping from terms to URLs containing that term
# The details of the index structure can be seen in the test cases

# check | 2 | https://cs.brown.edu/courses/csci1380/sandbox/1/level_1a/index.html
#url=$(echo -e "$2")
#echo -e "$2"
input=$(cat)
#echo -e "url recieved is : $1"
#echo -e "invert input is : $input"
index=$(echo -e "$input" | sort | uniq -c | awk -v url="$1" '{text = ""; for(i=2; i<=NF; i++) {text = text $i " "} print text "|", $1, "|", url}')
#awk -v url="$1" '{for(i=2; i<=NF; i++) {text = text $i " "} print text "|", $1}') #| awk -v url="$1" '{print $2 " " $3 " " $4 " | " $1 " | " url}')
echo -e "$index"