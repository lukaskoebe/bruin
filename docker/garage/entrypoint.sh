#!/bin/sh
set -eu

CONFIG_PATH="/etc/garage.toml"

/garage -c "$CONFIG_PATH" server &
GARAGE_PID=$!

cleanup() {
  kill "$GARAGE_PID" 2>/dev/null || true
  wait "$GARAGE_PID" 2>/dev/null || true
}

trap cleanup INT TERM

echo "Waiting for Garage status..."
until /garage -c "$CONFIG_PATH" status >/tmp/garage-status.txt 2>/tmp/garage-status.err; do
  sleep 2
done

if grep -q "NO ROLE ASSIGNED" /tmp/garage-status.txt; then
  NODE_ID=$(awk '
    /^==== HEALTHY NODES ====/{healthy=1; next}
    healthy && $1 == "ID" {next}
    healthy && NF > 0 {print $1; exit}
  ' /tmp/garage-status.txt)

  if [ -n "$NODE_ID" ]; then
    echo "Assigning single-node layout to Garage node $NODE_ID"
    /garage -c "$CONFIG_PATH" layout assign -z dc1 -c 1G "$NODE_ID"
    /garage -c "$CONFIG_PATH" layout apply --version 1
  fi
else
  echo "Garage layout already assigned"
fi

if [ -n "${GARAGE_DEFAULT_ACCESS_KEY:-}" ] && [ -n "${GARAGE_DEFAULT_SECRET_KEY:-}" ]; then
  if ! /garage -c "$CONFIG_PATH" key info "$GARAGE_DEFAULT_ACCESS_KEY" >/tmp/garage-key-info.txt 2>/tmp/garage-key-info.err; then
    echo "Importing default Garage access key"
    /garage -c "$CONFIG_PATH" key import --yes -n "Bruin default key" "$GARAGE_DEFAULT_ACCESS_KEY" "$GARAGE_DEFAULT_SECRET_KEY"
  else
    echo "Garage access key already exists"
  fi
fi

if [ -n "${GARAGE_DEFAULT_BUCKET:-}" ]; then
  if ! /garage -c "$CONFIG_PATH" bucket info "$GARAGE_DEFAULT_BUCKET" >/tmp/garage-bucket-info.txt 2>/tmp/garage-bucket-info.err; then
    echo "Creating default Garage bucket $GARAGE_DEFAULT_BUCKET"
    /garage -c "$CONFIG_PATH" bucket create "$GARAGE_DEFAULT_BUCKET"
  else
    echo "Garage bucket $GARAGE_DEFAULT_BUCKET already exists"
  fi

  if [ -n "${GARAGE_DEFAULT_ACCESS_KEY:-}" ]; then
    echo "Ensuring default Garage bucket permissions"
    /garage -c "$CONFIG_PATH" bucket allow --read --write --owner "$GARAGE_DEFAULT_BUCKET" --key "$GARAGE_DEFAULT_ACCESS_KEY"
  fi
fi

wait "$GARAGE_PID"