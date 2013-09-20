
echo=


checkForRole() {
	nroles=`igor list -roles $1 | wc -l`
	if [ -n "$nroles" -a "$nroles" -gt 0 ]
	then
		echo verified that $1 exists.
		return 0
	else
		echo verified that $1 does not exist.
		return 1
	fi
}
die() {
  echo $* 1>&2
  exit 1
}

renameRole() {
    oldrole=$1
    newrole=$2
    checkForRole $newrole && die Nonsense request: $newrole already exists.
    checkForRole $oldrole || die Nonsense request: $oldrole does not exist.
    
    $echo igor fetch -members $oldrole -definition > members.$newrole
    $echo igor create -role $newrole -yes -log "Making/renaming $oldrole->$newrole"
    $echo igor edit -members $newrole -file members.$newrole -log "Making/renaming $oldrole->$newrole"
    
    for i in ".namenode" ".namenode2" ".jobtracker" ".gateway"
    do
       oldname=members.${oldrole}${i}
       newname=members.${newrole}${i}
       if checkForRole $oldrole${i}
       then
          if checkForRole ${newrole}${i}
          then
             igor delete -role ${newrole}${i} -yes -log "remove target before a rename."
          fi
          $echo igor fetch -members $oldrole${i} -definition > $newname
          $echo igor create -role $newrole${i} -yes -log "Making/renaming $oldrole->$newrole"
          $echo igor edit -members $newrole${i} -file $newname -log "Making/renaming $oldrole->$newrole"
          $echo igor fetch -members $newrole${i} -file $newname -log "Making/renaming $oldrole->$newrole"
       fi
    done
}
L="
"
for l in $L
do  
    echo ">>>>>>> " Processing $l
          if checkForRole grid_re.clusters.$l
          then
             echo igor delete -role grid_re.clusters.$l -yes -log "remove target before a rename."
          fi
    renameRole grid_re.clusters.${l} grid_re.clusters.france
    echo "<<<<<<< " Finished with $l
done
    # renameRole grid_re.clusters.bigd  grid_re.clusters.france
    # renameRole grid_re.clusters.bigc  grid_re.clusters.italy
    # renameRole grid_re.clusters.bigb  grid_re.clusters.japan
    # renameRole grid_re.clusters.biga  grid_re.clusters.china
    # renameRole grid_re.clusters.biga  grid_re.clusters.asia
    # renameRole grid_re.clusters.asia  grid_re.clusters.europe
    renameRole grid_re.clusters.asia  grid_re.clusters.gaia
