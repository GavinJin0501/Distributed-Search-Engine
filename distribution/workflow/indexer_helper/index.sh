#!/bin/bash
# index.sh runs the core indexing pipeline
process="$4/process.sh"
stem="$4/stem.js"
combine="$4/combine.sh"
invert="$4/invert.sh"
merge="$4/merge.js"

echo "$1" |
  $process |
  $stem |
  $combine |
  $invert "$2" |
  $merge "$3" |
  sort -o "$3"

# echo "$1" |
#   ./process.sh |
#   ./stem.js |
#   ./combine.sh |
#   ./invert.sh "$2" |
#   ./merge.js "$3" |
#   sort -o "$3"