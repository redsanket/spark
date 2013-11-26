
# Level Owner, permission mode
#users: hadoopqa, cwchung
#group: users, hdfs
#hadoopqa users




SUDO=""
export PERM_TOP_DIR=/tmp/PermTest2
# export HDFS_PERM_TOP_DIR=/tmp/PermTest2
export HDFS_PERM_TOP_DIR=/user/hadoopqa/hdfsRegressionData/hdfsTestData/PermTest2
export PERM_DIRS_TXT=$HDFT_TOP_DIR/src/etc/perm_dirs.txt
export PERM_FILES_TXT=$HDFT_TOP_DIR/src/etc/perm_files.txt
export RDFILE=smallFile14Byte
export WRFILE=smallFile11Byte

export D_PERM="700 744 766 776 777"
export F_PERM="400 440 600 640 644 660 664 666"
export O_PERM="hdfsqa hadoopqa"
export G_PERM="hdfs users"

#######################################################################################
function createHdfsDirs {
	DIR_LIST=`cat $PERM_DIRS_TXT | sed -e "s#^#${HDFS_PERM_TOP_DIR}/#"`
	echo hdfs dfs -mkdir -p $DIR_LIST
	hdfs dfs -mkdir -p $DIR_LIST
}

function createHdfsFiles {
	FILE_LIST=`cat $PERM_FILES_TXT | sed -e "s#^#${HDFS_PERM_TOP_DIR}/#"`
	echo "#### %%%%%%%%%%%%%%%%%%%% ####"
	echo "#### %%%%%%%%%%%%%%%%%%%% ####"

	# if dest diredctory exist, -copyFromLocal need to strip off the PermTest2/
	hadoop dfs -stat $HDFS_PERM_TOP_DIR
	statExec=$?
	if [ $statExec == 0 ]; then
		COPY_DEST=`dirname $HDFS_PERM_TOP_DIR`
	else
		COPY_DEST=$HDFS_PERM_TOP_DIR
	fi
		
	echo "Copy to dest: $COPY_DEST ..."
	echo "#### %%%%%%%%%%%%%%%%%%%% ####"

	echo hdfs dfs -copyFromLocal $PERM_TOP_DIR    $COPY_DEST
	echo "#### %%%%%%%%%%%%%%%%%%%% ####"
	hdfs dfs -copyFromLocal $PERM_TOP_DIR         $COPY_DEST
	hdfs dfs -lsr $HDFS_PERM_TOP_DIR
}

#######################################################################################
function createLocalDirs {
	mkdir $PERM_TOP_DIR
	DIR_LIST=`cat $PERM_DIRS_TXT | sed -e "s#^#${PERM_TOP_DIR}/#"`
	echo mkdir $DIR_LIST
	mkdir $DIR_LIST
}

# This is done such that the creation of HDFS files can be accomplished using copyFromLocal in one-shot
function createLocalFiles {
	FILE_LIST=`cat $PERM_FILES_TXT | sed -e "s#^#${PERM_TOP_DIR}/#"`
	echo "createLocalFiles FILE_LIST"
	for f in $FILE_LIST; do
	    cp  $RDFILE $f
	done
	# ls -R $PERM_TOP_DIR

}

function listFNames {
	for owner in hdfsqa hadoopqa; do
   	    for group in hdfs users; do 
		for Dir in Dir-700 Dir-744 Dir-766 Dir-776 Dir-777; do
		    DirName=${owner}-${group}-${Dir}
		    # echo "DirName=$DirName"
			for P in 400 440 600 640 644 660 664 666 ; do 
			    echo $DirName/hadoopqa-hdfs-File-$P
			    echo $DirName/hadoopqa-users-File-$P
			    echo $DirName/hdfsqa-hdfs-File-$P
			    echo $DirName/hdfsqa-users-File-$P
			done
		done
	    done
	done

}

function listDNames {
	for owner in hdfsqa hadoopqa; do
   	    for group in hdfs users; do 
		for Dir in Dir-700 Dir-744 Dir-766 Dir-776 Dir-777; do
		    DirName=${owner}-${group}-${Dir}
		    echo $DirName
		done
	    done
	done
}

function changeHdfsOwner {
	for OWNER in $O_PERM ; do
		FILE_LIST=`cat $PERM_FILES_TXT  | sed -e "s#^#${HDFS_PERM_TOP_DIR}/#" | grep "/${OWNER}-[a-z]*-File" `
		echo hdfs dfs -chown $OWNER  $FILE_LIST
		hdfs dfs -chown $OWNER  $FILE_LIST
	done
	for OWNER in $O_PERM ; do
		FILE_LIST=`cat  $PERM_DIRS_TXT | sed -e "s#^#${HDFS_PERM_TOP_DIR}/#" | fgrep "/${OWNER}-[a-z]*-Dir" `
		echo hdfs dfs -chown $OWNER  $FILE_LIST
		hdfs dfs -chown $OWNER  $FILE_LIST
	done
}

function changeHdfsGroup {
	for GROUP in $G_PERM ; do
		FILE_LIST=`cat $PERM_FILES_TXT $PERM_DIRS_TXT | sed -e "s#^#${HDFS_PERM_TOP_DIR}/#" | egrep -- "-${GROUP}-File|-${GROUP}-Dir-" `
		echo hdfs dfs -chgrp $GROUP  $FILE_LIST
		hdfs dfs -chgrp $GROUP  $FILE_LIST
	done
}



function changeHdfsDirPerm {
	for P in $D_PERM ; do
            DIR_LIST=`cat $PERM_DIRS_TXT | sed -e "s#^#${HDFS_PERM_TOP_DIR}/#"  | grep "Dir-$P" `
            echo $P 
	    echo $DIR_LIST
	    hdfs dfs -chmod $P $DIR_LIST
	done
}


function changeHdfsFilePerm {

	for P in $F_PERM ; do
            FILE_LIST=`cat $PERM_FILES_TXT | sed -e "s#^#${HDFS_PERM_TOP_DIR}/#"  | grep "File-$P" `
            echo $P 
	    echo $FILE_LIST
	    hdfs dfs -chmod $P $FILE_LIST
	done
}


# test to record expected pass and expected erros
testHdfs() {
	echo "hadoop fs -lsr /user/hadoopqa/hdfsRegressionData/hdfsTestData/PermTest2  2> perm-lsr.err  1>perm-lsr.out"
	hadoop fs -lsr /user/hadoopqa/hdfsRegressionData/hdfsTestData/PermTest2  2> perm-lsr.err  1>perm-lsr.out
	echo 'hadoop fs -cat ` cat perm_files.txt | sed -e \"s#^#$TOP_DIR/#\"  2 perm-cat.err 1perm-cat.out'
	hadoop fs -cat ` cat perm_files.txt | sed -e "s#^#$TOP_DIR/#" ` 2> perm-cat.err 1>perm-cat.out

}


mkHdfs() {
	# now create the HDFS dir
	export TOP_DIR=/user/hadoopqa/hdfsRegressionData/hdfsTestData/PermTest2/

	which hadoop

	# create local dir and files such that can use dfs -copyFromLocal in one-shot
	createLocalDirs
	createLocalFiles
	ls -R /tmp/PermTest2
	# createHdfsDirs
	createHdfsFiles

	changeHdfsFilePerm
	changeHdfsDirPerm
	##  changeHdfsOwner
	## changeHdfsGroup
	## hadoop fs -lsr $HDFS_PERM_TOP_DIR
}

function hdftSuperuserKInit {
	WHOAMI=`whoami`
	if [ $WHOAMI == "hadoop8" ] || [ $WHOAMI == "hdfs" ] || [ $WHOAMI == "cwchung" ] ; then
		export EFFECTIVE_KB_USER="hdfs"
		# echo "    sudo -u hdfs kinit -k -t /etc/grid-keytabs/hdfs.dev.service.keytab $*   hdfs/dev.ygrid.yahoo.com@DEV.YGRID.YAHOO.COM"
		# sudo -u hdfs kinit -k -t /etc/grid-keytabs/hdfs.dev.service.keytab $*   hdfs/dev.ygrid.yahoo.com@DEV.YGRID.YAHOO.COM

		echo "    kinit -k -t /homes/hdfs/hdfs.dev.service.keytab $*   hdfs/dev.ygrid.yahoo.com@DEV.YGRID.YAHOO.COM"
		kinit -k -t /homes/hdfs/hdfs.dev.service.keytab $*   hdfs/dev.ygrid.yahoo.com@DEV.YGRID.YAHOO.COM
		klist | grep "Default principal: hdfs/dev.ygrid.yahoo.com@DEV.YGRID.YAHOO.COM"
		stat=$?
		if [ $stat == 0 ] ; then
			echo "    Effective KB user: hdfs"
		else 
			echo "    ERROR: Effective KB user is  not hdfs"
			echo "    ERROR: Effective KB user is  `klist | grep Default principal`"
		fi
	else
		echo "ERROR: Invalid user $WHOAMI to try to be a superuser in hdfs"
		echo "ERROR: Invalid user $WHOAMI to try to be a superuser in hdfs"
	fi
	hdftKList
}

function hdftSuperuserKDestroy {
	WHOAMI=`whoami`
	if [ $WHOAMI == "hadoop8" ] || [ $WHOAMI == "hdfs" ] || [ $WHOAMI == "hadoop9" ]  || [ $WHOAMI == "cwchung" ] ; then
	      kdestroy
	else
		echo "ERROR: Invalid user $WHOAMI to try to be a superuser in hdfs"
		echo "ERROR: Invalid user $WHOAMI to try to be a superuser in hdfs"
	fi
	hdftKList
}

###########################################
# Function to do klist: just show the principal, unless -l option is passed in
###########################################
function hdftKList {
	arg=$1

	if [ -n $arg ] ; then
		klist | cat -n 
	fi
	P=`klist |grep 'Default principal'`
	if [ $? == 0 ] ; then
		echo "    KLIST: Currently running as effective user:  [$P]"
	else
		echo "    KLIST: NO effective user principal $P running."
	fi
}



echo "To create HDFS Files .... "
mkHdfs
exit
hdftSuperuserKInit
klist 
sleep 10
changeHdfsOwner
changeHdfsGroup
hadoop fs -lsr $HDFS_PERM_TOP_DIR
hdftSuperuserKDestroy

echo "Done..."
