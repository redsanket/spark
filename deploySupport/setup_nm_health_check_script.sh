#!/bin/bash

# Move the existing file if it exists
script="/home/gs/conf/local/nm_health_check"
if [[ -f $script ]]; then
  mv $script $script.orig.`date +%s`
fi

(
    echo "#!/bin/bash"
    echo "echo 'node is healthy'"
    echo "exit 0"
) > $script
chmod 777 $script
