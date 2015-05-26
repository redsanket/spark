# copy the mgr.sh and support files to OOZIE_HOST and start mgr.sh

# get random tmp dir to place on OOZIE_HOST
TMPDIR=/tmp/stackint_jenkins_$RANDOM
echo "TMPDIR is: $TMPDIR"

# setup OOZIE_HOST
echo "OOZIE_HOST is: $OOZIE_HOST"
ssh  $OOZIE_HOST  "mkdir $TMPDIR"
scp -r Prototype/* $OOZIE_HOST:$TMPDIR

(
echo "cd $TMPDIR && ./mgr.sh"
)| ssh $OOZIE_HOST

