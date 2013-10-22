#!/bin/bash

sudo /home/y/bin/yroot console --cmd "HOSTNAME=`hostname` yinst stop ygrid_gdm_console_server"

sudo mv /grid/0/yroot/var/yroots/console/home/y/libexec/yjava_tomcat/webapps/logs/console-application.log /grid/0/yroot/var/yroots/console/home/y/libexec/yjava_tomcat/webapps/logs/console-application.log.${D}

wait

/home/y/etc/htf_gdm_console_restart/conf/gdm_update_facet_properties

sudo /home/y/bin/yroot console --cmd "yinst restart webctl"

sudo /home/y/bin/yroot console --cmd "HOSTNAME=`hostname` yinst start ygrid_gdm_console_server"