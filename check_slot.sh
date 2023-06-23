#!/bin/bash

start_time=$(date +%s)

local=$(/opt/solana/solana-release/bin/solana slot --url http://127.0.0.1:8899)
remote=$(/opt/solana/solana-release/bin/solana slot --url devnet)

echo -e "local\t$local"
echo -e "remote\t$remote"

if [ "$local" == "" ]
then
  local=0
fi

let "dif=$remote-$local"
echo -e dif'\t'$dif

while [ "$dif" -ge 5 ]
do
  local=$(/opt/solana/solana-release/bin/solana slot --url http://127.0.0.1:8899)
  remote=$(/opt/solana/solana-release/bin/solana slot --url devnet)

  echo -e "local\t$local"
  echo -e "remote\t$remote"

  if [ "$local" == "" ]
  then
    local=0
  fi

  let "dif=$remote-$local"
  echo -e dif'\t'$dif

  sleep 1
done

end_time=$(date +%s)
elapsed=$(( end_time - start_time ))

echo "Reached network slot in $elapsed seconds."
