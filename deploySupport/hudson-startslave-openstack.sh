#!/bin/bash

# hudson-startslave.sh now supports deployments to both hardware and VM clusters.
# This wrapper script is intended to temporarily support existing jenkins jobs
# that have been setup to call hudson-startslave-openstack.sh directly. Future
# jobs should call hudson-startslave.sh directly.

echo "WARNING: This script $0 has been deprecated as of 8/15/2016."
echo "WARNING: Please call hudson-startslave.sh directly in the future!!!"
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
command="$SCRIPT_DIR/hudson-startslave.sh"
if [ $# -gt 0 ]; then
    command+=" $@"
fi
echo "Run '$command':"
$command
EC=$?
echo "Finished running '$command': exit code='$EC'."
exit $EC
