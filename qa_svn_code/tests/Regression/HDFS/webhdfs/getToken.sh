host=$1
/usr/bin/curl -L -u : --negotiate "$host"  | grep -Po 'urlString":.*' | cut -d: -f2 | cut -d} -f1 | cut -d\" -f2