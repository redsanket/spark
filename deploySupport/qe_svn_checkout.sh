target_host=$1
target_host=${target_host:-gwbl2005.blue.ygrid.yahoo.com}

dest_dir=$2
dest_dir=${dest_dir:-/home/y/var/builds/workspace/NightlyHadoopQEAutomation-23}

artifacts_dir=$3
artifacts_dir=${artifacts_dir:-$HOMEDIR/hadoopqa/hudson_artifacts/hudson_artifacts-0.23}

svn_repo=svn+ssh://svn.corp.yahoo.com/yahoo/platform/grid/projects/trunk/hudson/internal/HadoopQEAutomation/branch-23
cd ${WORKSPACE}
rm -rf qa_svn_code_23
/home/y/bin/svn co $svn_repo qa_svn_code_23
tar cvfz qa_svn_code_23.tgz qa_svn_code_23

set -e
set -x

# Check the root dir
ssh $target_host "if [ ! -d $dest_dir ];then echo "directory $dest_dir is missing: create it"; /bin/mkdir -p $dest_dir; else echo "directory $dest_dir exists";fi"

# Clean up the directory
ssh $target_host "rm -rf $dest_dir/*"

scp qa_svn_code_23.tgz $target_host:$dest_dir
ssh $target_host "cd $dest_dir && tar xvfz qa_svn_code_23.tgz"
ssh $target_host "cd $dest_dir && mv qa_svn_code_23/* ."
#ssh $target_host  "chmod -R 775  $dest_dir"

# ssh $target_host run a script to do the following

# In order to aggregate tests results for Hudson jobs that ran on parallel
# clusters, we need to link the artifacts root directory in the workspace to the
# root directory in /home/hadoopqa which is NFS mounted across all the gateways.
ssh $target_host "if [ ! -d $artifacts_dir ];then /bin/mkdir -p $artifacts_dir; fi; /bin/ln -s $artifacts_dir $dest_dir/hudson_artifacts"
