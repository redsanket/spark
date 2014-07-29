#!/bin/sh

set -x

echo ================= evaluating whether to install pig
echo ================= PIGVERSION = $PIGVERSION
echo ================= pignode = $PIGNODE
echo ================= yroothome = $YROOTHOME

export PIG_HOME=

case "$PIGVERSION" in
  none)
      echo === not installing pig at all.
      ;;
  *pig*)
      echo === installing pig version $PIGVERSION to $PIGNODE 
      ssh $PIGNODE /usr/local/bin/yinst install -root $YROOTHOME $PIGVERSION -br quarantine
      st=$?
      if [ "$st" -eq 0 ] ; then
         echo "Setting PIGHOME to $YROOTHOME"
         export PIG_HOME=$YROOTHOME
      else
         echo $st &&echo "*****" PIG NOT INSTALLED "*****" && exit $st
      fi
      ;;
   *)
      echo === "********** ignoring pigversion=$PIGVERSION"
      ;;
esac
