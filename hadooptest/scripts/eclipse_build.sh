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

readonly MvnOptions="-gs $ScriptDir/../resources/yjava_maven/settings.xml.orig -Dmaven.compiler.source=1.8 -Dmaven.compiler.target=1.8"
pushd "$MavenProjectDir"
# This is to work around the work-around for building with Spark 1.1.
# https://git.corp.yahoo.com/HadoopQE/hadooptest/blob/7eaf425c70950a46d0ad3a5b810ffe8391a72b4c/Makefile#L14
rm -vrf "$ScriptDir/../htf-common/src/test/scala/hadooptest/spark/regression/spark1_2"
set +e
mvn $MvnOptions eclipse:clean && \
    mvn $MvnOptions install -DskipTests -Pprofile-corp -Pprofile-all -Dmaven.compiler.source=1.8 -Dmaven.compiler.target=1.8  && \
    mvn $MvnOptions eclipse:eclipse -Pprofile-corp -Pprofile-all -Dmaven.compiler.source=1.8 -Dmaven.compiler.target=1.8 
readonly ExitCode="$?"
popd

echo Done.
exit $ExitCode
