#!/bin/bash
# index.sh runs the core indexing pipeline
#echo "$1"

merge="$2/merge.js"
scriptLog="$2/script_log.txt"

  echo "$1" |
  $merge "$3" |
  sort -o "$3"
