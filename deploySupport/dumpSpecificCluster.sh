#!/usr/local/bin/bash

role=${CLUSTER:-ankh}

p=`dirname $0`
export PATH=${p}:$PATH

listAllRoles() { 
   dumpAllRoles.sh
}
listSpecificRoles() { 
   dumpAllRoles.sh | egrep $*
}
dumpSpecificRoles() { 
   x=` dumpAllRoles.sh | egrep $*`
   if [ -z "$x" ]
   then
      echo "$*: no clusters found." 1>&2
      exit 1
   else
      echo ===== igor list -roles \"${*}\*\"
      for r in $x
      do
          echo $r
      done
   fi
   for r in $x
   do
       echo ===== igor fetch -members $r
       dumpMembershipList.sh $r
   done
}

echo '===== 0.22 cluster definitions follow.'
dumpSpecificRoles "grid_re.clusters.${role}"

exit 0
