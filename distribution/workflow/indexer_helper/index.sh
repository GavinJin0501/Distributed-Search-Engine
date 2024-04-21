#!/bin/bash
# index.sh runs the core indexing pipeline

echo "$1" |
  indexer_helper/process.sh |
  indexer_helper/stem.js |
  indexer_helper/combine.sh |
  indexer_helper/invert.sh "$2" |
  indexer_helper/merge.js "$3" |
  sort -o "$3"
