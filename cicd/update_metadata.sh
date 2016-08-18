#!/bin/sh

set -x

if [ $# -eq 0 ]
  then
    echo "No branch supplied"
    exit -1
fi

echo "Creating build info..."
echo "Screwdriver: ${SCREWDRIVER}" > ${SOURCE_DIR}/cicd/gridci-metadata.txt
echo "Build Description: `cat ${SOURCE_DIR}/RELEASE`" >> ${SOURCE_DIR}/cicd/gridci-metadata.txt
echo "BUILD_URL=${BUILD_URL}" >> ${SOURCE_DIR}/cicd/gridci-metadata.txt
echo "REPO=${GIT_URL}" >> ${SOURCE_DIR}/cicd/gridci-metadata.txt
echo "COMMIT=${GIT_COMMIT}" >> ${SOURCE_DIR}/cicd/gridci-metadata.txt
echo "BRANCH=${GIT_BRANCH}" >> ${SOURCE_DIR}/cicd/gridci-metadata.txt
echo "JOB_NAME=${JOB_NAME}" >> ${SOURCE_DIR}/cicd/gridci-metadata.txt
echo "BUILD_NUMBER=${BUILD_NUMBER}" >> ${SOURCE_DIR}/cicd/gridci-metadata.txt

echo "Updating cicd/gridci-metadata.txt file..."
git branch
git status
git add . && git commit -m "Updating cicd metadata" cicd/gridci-metadata.txt
git push origin $1
rc=$?; if [[ $rc != 0 ]]; then exit -1; fi
echo "Metadata update completed!"
