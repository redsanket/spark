cat <<XX

 text from "219-installsteps-jobtrackerExplanation.sh"

 *****************************************************************
 *****************************************************************
 *****************************************************************

          How the job tracker is started.

The following two scripts configure and then start the job-tracker:
   220-installsteps-configureJT.sh
       Runs a script in mapred-conf-dir to reconfigure the directory
       from a slave version to a job-tracker version. Uses a lot
       of yinst-set commands to do the delta. This script is
       cluster-specific.
   230-installsteps-startJT.sh
       copies jobtrackerscriptgrid.sh to the job-tracker machine,
       and it runs the standard open-source scripts to start/stop
       mapred.

 *****************************************************************
 *****************************************************************
 *****************************************************************

XX
