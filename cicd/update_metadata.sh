#!/bin/sh

echo "Build Description: `cat ${WORKSPACE}/RELEASE`" > ${SOURCE_DIR}/cicd/gridci-metadata.txt
echo "VERSION=${HTF_VERSION}" >> ${SOURCE_DIR}/cicd/gridci-metadata.txt
echo "BUILD_URL=${BUILD_URL} >> ${SOURCE_DIR}/cicd/gridci-metadata.txt
echo "COMMIT=${GIT_COMMIT} >> ${SOURCE_DIR}/cicd/gridci-metadata.txt
echo "BRANCH=${GIT_BRANCH} >> ${SOURCE_DIR}/cicd/gridci-metadata.txt
echo "${JOB_NAME}:${BUILD_NUMBER}=`cat ${WORKSPACE}/RELEASE`" >> ${SOURCE_DIR}/cicd/gridci-metadata.txt

echo "Updating cicd from branch $1 to branch $2..."
git branch
git status
git commit -am "Updating cicd metadata" ${SOURCE_DIR}/cicd/gridci-metadata.txt
git push origin master
rc=$?; if [[ $rc != 0 ]]; then exit -1; fi

# change branch back to original
git checkout $1
git stash pop
echo "Metadata update completed!"
