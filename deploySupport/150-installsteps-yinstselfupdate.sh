set +x

if [ "$YINSTSELFUPDATE" = false ]; then
    echo "YINSTSELFUPDATE is not enabled. Not doing yinst self-update"
    return 0
fi

echo "Run yinst self-update"
fanout_root "yinst self-update -branch rhel6stable -yes"
fanoutGW_root "yinst self-update -branch rhel6stable -yes"
