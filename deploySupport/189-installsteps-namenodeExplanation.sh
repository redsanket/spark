cat <<XX

 text from "189-installsteps-namenodeExplanation.sh":

 *****************************************************************
 *****************************************************************
 *****************************************************************

The namenode mechanism in this deploy follows this steps:
(1) Retrieve the list of namenodes from Igor. It is:
XX
echo "    " $namenode | fmt
cat <<YY
(2) If we need to identify any primary namenode, for example
    to retrieve a cluster-id, we'll go to the first one on the list.
(3) The order of worship is:
    190-installsteps-runNNkinit.sh (on each namenode, run kinit)
    200-installsteps-configureNN.sh (on each namenode, run
        a script to config hadoop-conf-dir as a namenode config
        directory. Note that the Federation configurations were done
        earlier, as part of the installation of the config package.
        The shell-commands to create the Federated config are in 
        001-create-conf-installscript.sh and to run them,
        170-installsteps-installmainpackages.sh)
    205-installsteps-getClusterid.sh (on the first namenode, get the
        cluster-id. Put it into a shell variable.)
    210-installsteps-startNN.sh (on each namenode, run scripts to start
        namenode, datanodes, etc.)

	-last updated on Fri Jan 14 22:52:23 PST 2011

 *****************************************************************
 *****************************************************************
 *****************************************************************

YY
