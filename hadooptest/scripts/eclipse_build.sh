#!/bin/bash

set -e

# Adapted from:
# http://stackoverflow.com/questions/1055671/how-can-i-get-the-behavior-of-gnus-readlink-f-on-a-mac
function readlink_f() {
    local TARGET_FILE="$1"

    cd "$(dirname "$TARGET_FILE")"
    TARGET_FILE="$(basename "$TARGET_FILE")"

    # Iterate down a (possible) chain of symlinks
    while [ -L "$TARGET_FILE" ]
    do
        TARGET_FILE="$(readlink "$TARGET_FILE")"
        cd "$(dirname "$TARGET_FILE")"
        TARGET_FILE="$(basename "$TARGET_FILE")"
    done

    # Compute the canonicalized name by finding the physical path
    # for the directory we're in and appending the target file.
    local readonly PHYS_DIR="$(pwd -P)"
    local readonly RESULT="$PHYS_DIR/$TARGET_FILE"
    echo "$RESULT"
}

readonly ScriptDir="$(dirname "$(readlink_f "$0")")"
readonly MavenProjectDir="$ScriptDir/.."

pushd "$MavenProjectDir"
set +e
mvn eclipse:clean && \
    mvn install -DskipTests -Pprofile-corp -Pprofile-all && \
    mvn eclipse:eclipse -Pprofile-corp -Pprofile-all
readonly ExitCode="$?"
popd

echo Done.
exit $ExitCode
