#################################################################################
# Scripts
#################################################################################

hudson-startslave.sh
	The first script called by Hudson. It massages the arguments
	given, source'ing massageHudsonParameters.sh, then creates a
	yinst-package of the scripts needed (by calling yinstify.sh),
	then copies that package to the destination machine and runs it,
	which runs installgrid.sh on adm102.
yinstify.sh
	This packages up the current directory into a package called
	"hadoopgridrollout" and makes the default values (in a .yicf)
	match the current environment settings.  The implication is that
	this specific (new) package, which is never installed in 'dist',
	can replay the same installation onto the same cluster with the
	same options, again and again.
installgrid.sh

        (need to rewrite since we've pulled many small blocks into files
        of their own.)

	The script that first calls "cluster-list.sh" to decide which
	machines correspond to the requested cluster, and then starts
	the process of cleaning-up, then installing, then configuring,
	the packages.  Note that some of the command sequences are in
	sub-files (namenode-part-1-script.sh, namenode-part-2-script.sh,
	namenode-part-3-script.sh) that are rsync'ed to the target
	machines and run.
	The last thing that this script does, is scp a test-script
	to the gateway and run it: "sh /tmp/scriptname.sh -c ankh -w"
	is an example.  There are 3-4 different script possibilities,
	listed below:
get-tests-and-run.sh
	install the package, hadoopvalidation, and then run
	bin/main3.sh there. It will futz with a single executable
	along the way, to make wordcount_simple function correctly.
shorttest-rw.sh
	the smallest three-task validation: a NN, a DN, and JT/TT task.

