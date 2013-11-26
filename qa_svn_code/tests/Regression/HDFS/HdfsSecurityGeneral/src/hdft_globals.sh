
# global variables which are meant to be read and write
# env varaible and dir setting do not belong to this file

# each run conists of a number of jobs, in each of the job_script directiory. Each job consists of a number of task.

# Configured env: TRACE0 gives more details then DEBUG, in turns more details than VERBOSE
# VERBOSE: mostly transaction level: echo env, command argument, give more details in exec sub-tasks
# DEBUG: use for debugging the scripts
# TRACE: use for detailing tracing for debugging

# Do it this way so that it can be overridden by env setting
if [ -z "$HDFT_VERBOSE" ] ; then
	export HDFT_VERBOSE=1
fi
if [ -z "$HDFT_DEBUG" ] ; then
	export HDFT_DEBUG=0
fi
if [ -z "$HDFT_TRACE" ] ; then
	export HDFT_TRACE=0
fi

# JOBS_RESULT: accumulation of results of jobs
export HDFT_JOBS_RESULT=0

# accumulation of RESULT of tasks (of one job)
export HDFT_TASKS_RESULT=0

# result from each task
export HDFT_RESULT=0
