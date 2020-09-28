cat <<XX

 text from "000-installsteps-explanation.sh":

 *****************************************************************
 *****************************************************************
 *****************************************************************
Keep in mind that the deploy-job runs on a Jenkins worker node.
It then makes a yinst-package on the fly, with the Hudson field
names/variables exported to the package as YINST SET variables, and
scp's the package to $ADMIN_HOST and installs/starts it.

On $ADMIN_HOST, now running as root, it has the access
($ADMIN_HOST has .ssh access to the entire QA/QE
machines as root) and privs (yinst runs as root) to run installgrid.sh.

That weird little dance is what grid-ops wants, to guarantee that people
are not tromping around on $ADMIN_HOST with root interactive shells.

	-last updated on Thu Jan  6 16:42:49 PST 2011
 *****************************************************************
 *****************************************************************
 *****************************************************************

XX
