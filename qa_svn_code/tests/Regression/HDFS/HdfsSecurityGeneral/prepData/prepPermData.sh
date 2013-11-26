### Top or Key Directory setting

# this is the master directory from one source, that is to be copied to various cluster 
source  ../src/hdft_util_lib.sh

echo "Accept argument: hd_top_Dir to populate the directory"
echo "Called by prepData.sh, with dest dir set up, and kinit done"

DFS_CMD=`hdft_getDfsCommand`
echo "   Env  DFS_CMD=$DFS_CMD"

if [ $# != 1 ] ; then
   DEST_DIR=/user/hadoopqa/hdfsRegressionData/PermTest2
else
   DEST_DIR=$1/PermTest2
fi


export HDFS_PERM_TOP_DIR=$DEST_DIR

export PERM_DIRS_TXT=perm-dirlist.txt
export PERM_FILES_TXT=perm-filelist.txt

export D_PERM="700 744 766 776 777"
export F_PERM="400 440 600 640 644 660 664 666"
export O_PERM="hdfsqa hadoopqa"
export G_PERM="hdfs users"

#######################################################################################
export RDFILE=smallFile14Byte
export WRFILE=smallFile11Byte

export D_PERM="700 744 766 776 777"
export F_PERM="400 440 600 640 644 660 664 666"
export O_PERM="hdfsqa hadoopqa"
export G_PERM="hdfs users"

#######################################################################################

function changeHdfsOwner {
	for OWNER in $O_PERM ; do
		FILE_LIST=`cat $PERM_FILES_TXT  | sed -e "s#^#${HDFS_PERM_TOP_DIR}/#" | grep "/${OWNER}-[a-z]*-File" `
		echo $DFS_CMD dfs -chown $OWNER  $FILE_LIST
		$DFS_CMD dfs -chown $OWNER  $FILE_LIST
	done
	for OWNER in $O_PERM ; do
		FILE_LIST=`cat  $PERM_DIRS_TXT | sed -e "s#^#${HDFS_PERM_TOP_DIR}/#" | fgrep "/${OWNER}-[a-z]*-Dir" `
		echo $DFS_CMD dfs -chown $OWNER  $FILE_LIST
		$DFS_CMD dfs -chown $OWNER  $FILE_LIST
	done
}

function changeHdfsGroup {
	for GROUP in $G_PERM ; do
		FILE_LIST=`cat $PERM_FILES_TXT $PERM_DIRS_TXT | sed -e "s#^#${HDFS_PERM_TOP_DIR}/#" | egrep -- "-${GROUP}-File|-${GROUP}-Dir-" `
		echo $DFS_CMD dfs -chgrp $GROUP  $FILE_LIST
		$DFS_CMD dfs -chgrp $GROUP  $FILE_LIST
	done
}



function changeHdfsDirPerm {
	for P in $D_PERM ; do
            DIR_LIST=`cat $PERM_DIRS_TXT | sed -e "s#^#${HDFS_PERM_TOP_DIR}/#"  | grep "Dir-$P" `
            echo $P 
	    echo $DIR_LIST
	    $DFS_CMD dfs -chmod $P $DIR_LIST
	done
}


function changeHdfsFilePerm {

	for P in $F_PERM ; do
            FILE_LIST=`cat $PERM_FILES_TXT | sed -e "s#^#${HDFS_PERM_TOP_DIR}/#"  | grep "File-$P" `
            echo $P 
	    echo $FILE_LIST
	    $DFS_CMD dfs -chmod $P $FILE_LIST
	done
}



echo "To create HDFS Files .... "
hdftSuperuserKInit
klist 
changeHdfsFilePerm
changeHdfsDirPerm
changeHdfsOwner
changeHdfsGroup
hadoop fs -lsr $HDFS_PERM_TOP_DIR
hdftSuperuserKDestroy

echo "Done..."
