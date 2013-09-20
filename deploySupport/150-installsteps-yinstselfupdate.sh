if [ "$YINSTSELFUPDATE" = true ]
then
    echo == doing yinst self-update
    fanout "yinst self-update -branch rhel6stable -yes "
    fanoutGW "yinst self-update -branch rhel6stable -yes "
else
    echo == not doing yinst self-update
fi
