#!/usr/bin/env bash

# Start: Resolve Script Directory
SOURCE="${BASH_SOURCE[0]}"
while [ -h "${SOURCE}" ]; do # resolve $SOURCE until the file is no longer a symlink
   bin="$( cd -P "$( dirname "${SOURCE}" )" && pwd )"
   SOURCE="$(readlink "${SOURCE}")"
   [[ "${SOURCE}" != /* ]] && SOURCE="${bin}/${SOURCE}" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
bin="$( cd -P "$( dirname "${SOURCE}" )" && pwd )"
script=$( basename "${SOURCE}" )
# Stop: Resolve Script Directory

conf="${bin}/../conf/"
logs="${bin}/../logs/"
lib="${bin}/../lib/"
hostname="`hostname`"

mkdir -p "${logs}"

export CLASSPATH="${lib}/*:${conf}"

exec java org.apache.accumulo.randomwalk.Framework --configDir "${conf}" --logDir "${logs}" \
    --logId "${hostname}" --module Simple.xml 2>${logs}/randomwalk.err 1>${logs}/randomwalk.out &
