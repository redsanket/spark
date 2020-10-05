set +x

if [ "$YINSTSELFUPDATE" = false ]; then
    echo "YINSTSELFUPDATE is not enabled. Not doing yinst self-update"
    return 0
fi

echo "Run yinst self-update"
fanout "yinst self-update -branch rhel6stable -yes"
fanoutGW "yinst self-update -branch rhel6stable -yes"
