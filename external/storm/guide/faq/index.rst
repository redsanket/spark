FAQ
===
.. See also http://twiki.corp.yahoo.com/view/Grid/StormDocumentation#FAQ.

Questions
---------

- `Q: I am seeing "Connection reset by peer" errors and "Netty Client Reconnect" messages.`_
- `Q: My worker process resets a periodically with no indication of what happened in the logs.`_
- `Q: I am seeing "Authentication challenge without WWW-Authenticate header" errors in my topology.`_
- `Q: Which JDK version does yStorm support?`_
- `Q: What are the basic steps to launch Storm topologies?`_ 
- `Q: Why StormSummitter failed to find principal from Kerberos cache?`_
- `Q: Does Storm support dependency isolation?`_
- `Q: Are there any libraries that storm is not compatible with?`_

Answers
-------

Q: I am seeing "Connection reset by peer" errors and "Netty Client Reconnect" messages.
#######################################################################################

The messages usually take the form of the following::

    [INFO] b.s.m.n.Client:88 thd=netty-client-timer tplg= cmpn= trcid= msg= Reconnect ... [2] to {HOST}/{IP}:{PORT}
    or
    2014-07-07 07:49:35 [INFO] b.s.m.n.StormClientHandler:63 thd=New I/O worker #1 tplg= cmpn= trcid= msg= Connection to {HOST}/{IP}:{PORT} failed:
    java.io.IOException: Connection reset by peer
            at sun.nio.ch.FileDispatcherImpl.read0(Native Method) ~[na:1.7.0_51]
            at sun.nio.ch.SocketDispatcher.read(SocketDispatcher.java:39) ~[na:1.7.0_51]
            at sun.nio.ch.IOUtil.readIntoNativeBuffer(IOUtil.java:223) ~[na:1.7.0_51]
            at sun.nio.ch.IOUtil.read(IOUtil.java:192) ~[na:1.7.0_51]
            at sun.nio.ch.SocketChannelImpl.read(SocketChannelImpl.java:379) ~[na:1.7.0_51]
            at org.jboss.netty.channel.socket.nio.NioWorker.read(NioWorker.java:64) ~[netty-3.8.0.Final.jar:na]
            at org.jboss.netty.channel.socket.nio.AbstractNioWorker.process(AbstractNioWorker.java:108) ~[netty-3.8.0.Final.jar:na]
            at org.jboss.netty.channel.socket.nio.AbstractNioSelector.run(AbstractNioSelector.java:318) ~[netty-3.8.0.Final.jar:na]
            at org.jboss.netty.channel.socket.nio.AbstractNioWorker.run(AbstractNioWorker.java:89) ~[netty-3.8.0.Final.jar:na]
            at org.jboss.netty.channel.socket.nio.NioWorker.run(NioWorker.java:178) ~[netty-3.8.0.Final.jar:na]
            at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1145) [na:1.7.0_51]
            at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:615) [na:1.7.0_51]
            at java.lang.Thread.run(Thread.java:744) [na:1.7.0_51]

This is an indication that the worker process on ``{HOST}`` listening on port 
``{PORT}`` has gone down for some reason and this worker process is just letting 
you know. You should look in the logs for the worker process running on ``{HOST}:{PORT}`` 
to see if there is anything in there indicating why it went down. If there is 
nothing there and the logs look like they just stopped abruptly, see the
FAQ on :ref:`worker process resetting <>`.

Q: My worker process resets a periodically with no indication of what happened in the logs.
###########################################################################################

If your worker process dies with no indication as to why it died, it is probably 
the supervisor shooting it. The supervisor is not always that kind and will sometimes 
shoot a process with a kill -9. There are usually two reasons why the supervisor 
will shoot a worker. Either the worker has stopped heartbeating in, or nimbus has 
decided to reschedule it somewhere else.

The best way to know which is to look if the worker process starts up again on the 
same host and port. If it does then your worker probably stopped heartbeating. If 
it starts up somewhere else, then nimbus probably rescheduled it. Nimbus rescheduling 
is unlikely if your process is restarting regularly. If you think this is happening 
please contact the storm team and we can help debug what is going on.

If your worker stopped heartbeating the most likely suspect is java Garbage Collection. 
If the supervisor does not see heartbeats from your process for more then 5 seconds 
it assumes it is dead and will try to restart it. Heartbeats are on a separate 
high priority thread and really only full stop the world garbage collection tends 
to stop them. Please look at the size of the HEAP you are using, but be careful 
to not go over 3.5 GB without checking with the storm team first.

Q: I am seeing "Authentication challenge without WWW-Authenticate header" errors in my topology.
################################################################################################

The "Authentication challenge without WWW-Authenticate header" typically is because 
the YCA authentication filter violates the HTTP specification by returning a 
"not authenticated" response code without providing challenge information. Some 
HTTP clients return this, like the one that we have used with the RegistryService. 
This usually means that you included a YCAv2 header in the request to the registry 
service, but did not go through the HTTP proxy. We usually have this set on all 
the gateways by default. If you ran your topology from a hosted gateway and got 
this error please file a BUG in Low Latency to let us know. If it was from your 
launcher box you probably need to configure it. You can look HERE for the available 
HTTP proxies by colo.

If you are setting it for a launcher box you probably want to set it through 
yinst, with something like the following::

    yinst set "ystorm.http_registry_proxy=http://httpproxy-res.red.ygrid.yahoo.com:4080”

If you can also set ``"http.registry.proxy"`` manually either on the command line with ``-c``, 
or programatially in the conf map.

Q: Which JDK version does yStorm support?
#########################################

yStorm supports JDK7 on 64bit OS. On grid gateway, please make sure that you are using ``/home/gs/java/jdk64/current/``.

Q: What are the basic steps to launch Storm topologies?
#######################################################

#. ``kinit``
#. ``storm jar YourJar YourClass YourTopology``
#. Use your topology. 
#. ``storm kill YourTopology``

Q: Why StormSummitter failed to find principal from Kerberos cache?
##################################################################

Please make sure that you don't have any environment settings for krb5. Please check::

    set | grep -i krb5

If you find any krb5 key in the env, please unset them. Example::

    unset KRB5CCNAME

Q: Does Storm support dependency isolation?
###########################################

Storm does not currently support dependency isolation, and Storm's class path takes 
precedence over the topology jar. This means that for the time being you are limited 
in what you can have as a dependency. See the `full list of storms dependencies <https://git.corp.yahoo.com/storm/storm/blob/master-security/storm-core-mvn/pom.xml>`_. 

One common dependencies that may cause you problems is ``Guava``.

Q: Are there any libraries that storm is not compatible with?
#############################################################

After the 0.8.2 release storm switched to ``logback`` for it's logging framework 
with a shim layer that supports some of the ``log4j`` APIs. If you include log4j 
in your class path it has been known to cause issue.

