
# Level Owner, permission mode
#users: hadoopqa, cwchung
#group: users, hdfs

PERM_TOP_DIR=/tmp/PermTest3
PERM_DIRS_TXT=perm-dirlist.txt
PERM_FILES_TXT=perm-filelist.txt


#######################################################################################
function createLocalDirs {
	mkdir $PERM_TOP_DIR
	CUR_PWD=`pwd`
	cd ${PERM_TOP_DIR}
	#DIR_LIST=`cat $PERM_DIRS_TXT | sed -e "s#^#${PERM_TOP_DIR}/#"`
	DIR_LIST=`cat $PERM_DIRS_TXT`
	echo mkdir $DIR_LIST
	mkdir $DIR_LIST
	cd $CUR_PWD
}

# This is done such that the creation of HDFS files can be accomplished using copyFromLocal in one-shot
function createLocalFiles {
	# FILE_LIST=`cat $PERM_FILES_TXT | sed -e "s#^#${PERM_TOP_DIR}/#"`
	mkdir $PERM_TOP_DIR
	CUR_PWD=`pwd`
	cd ${PERM_TOP_DIR}
	FILE_LIST=`cat $PERM_FILES_TXT`
	echo "createLocalFiles FILE_LIST"
	for f in $FILE_LIST; do
	    cp  $CUR_PWD/$RDFILE $f
	done
	# ls -R $PERM_TOP_DIR

}

	createLocalDirs
	createLocalFiles
	ls -R $PERM_TOP_DIR
