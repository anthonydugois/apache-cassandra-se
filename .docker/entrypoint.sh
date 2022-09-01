#!/bin/bash

set -e

# first arg is `-f` or `--some-option`
# or there are no args
if [ "$#" -eq 0 ] || [ "${1#-}" != "$1" ]; then
  set -- cassandra -f "$@"
fi

# allow the container to be started with `--user`
if [ "$1" = 'cassandra' -a "$(id -u)" = '0' ]; then
  find "$CASSANDRA_CONF" /var/lib/cassandra /var/log/cassandra \
    \! -user cassandra -exec chown cassandra '{}' +
  exec gosu cassandra "$BASH_SOURCE" "$@"
fi

exec "$@"
