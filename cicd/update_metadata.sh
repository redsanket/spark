#!/bin/sh

set -x

if [ $# -eq 0 ]
  then
    echo "No branch supplied"
    exit -1
fi
git checkout $1
echo "Creating build info..."
echo "Screwdriver: ${SCREWDRIVER}" > ${WORKSPACE}/cicd/gridci-metadata.txt
echo "BUILD_URL=${BUILD_URL}" >> ${WORKSPACE}/cicd/gridci-metadata.txt
echo "REPO=${GIT_URL}" >> ${WORKSPACE}/cicd/gridci-metadata.txt
echo "COMMIT=${GIT_COMMIT}" >> ${WORKSPACE}/cicd/gridci-metadata.txt
echo "BRANCH=${GIT_BRANCH}" >> ${WORKSPACE}/cicd/gridci-metadata.txt
echo "JOB_NAME=${JOB_NAME}" >> ${WORKSPACE}/cicd/gridci-metadata.txt
echo "BUILD_NUMBER=${BUILD_NUMBER}" >> ${WORKSPACE}/cicd/gridci-metadata.txt

echo "Updating cicd/gridci-metadata.txt file..."
git branch
git status
git add . && git commit -m "Updating cicd metadata" cicd/gridci-metadata.txt
git push $2 $1
rc=$?; if [[ $rc != 0 ]]; then exit -1; fi
echo "Metadata update completed!"
