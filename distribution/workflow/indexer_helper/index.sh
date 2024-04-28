#!/bin/bash
# index.sh runs the core indexing pipeline
#echo "$1"

process="$4/process.sh"
stem="$4/stem.js"
combine="$4/combine.sh"
invert="$4/invert.sh"
merge="$4/merge.js"
scriptLog="$4/script_log.txt"


echo "$1" |
  $process "$4" | tee -a "$scriptLog" |
  $stem | tee -a "$scriptLog" |
  $combine | tee -a "$scriptLog" |
  $invert "$2" | tee -a "$scriptLog" |
  $merge "$3" | tee -a "$scriptLog" |
  sort -o "$3" | tee -a "$scriptLog"

# echo "$1" |
#   ./process.sh |
#   ./stem.js |
#   ./combine.sh |
#   ./invert.sh "$2" |
#   ./merge.js "$3" |
#   sort -o "$3"
