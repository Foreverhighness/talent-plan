#!/bin/bash

rm -rf tmp
mkdir -p tmp
n=60
m=36
for i in $(seq 1 $n); do
  for j in $(seq 1 $m); do
    RUST_LOG=info RUST_BACKTRACE=full cargo test --package raft --lib -- --test 2a --nocapture &> "tmp/2a-$i-$j.txt" &
  done
  sleep 1s
done

wait
rg 'passed' tmp/* | rg -v '3 passed'
rm -r tmp
