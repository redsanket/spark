========
Appendix
========

This chapter includes reference information to help you use Hive at Yahoo. 

.. toctree::
   :maxdepth: 2
   :hidden:

   ddl

.. _gs_appendix-grid_vips:

Grid VIP URLs/Ports
===================

The following table provides the URIs and ports to the Hive Server 2 and HCatalog
servers on each cluster.

For more information about the software installed on the different clusters,
see `Grid Versions <http://yo/GridVersions>`_.

.. _gs_appendix-hcat_servers:

HCat Servers
------------
+----------+----------------------------------------------------------------+-----------------------------------------------------------------------+--------------------+
| **Grid** | **HCatalog Thrift URI**                                        | **Kerberos Principal**                                                | **Comments**       |
+==========+================================================================+=======================================================================+====================+
| AR       | ``thrift://axonitered-hcat.ygrid.vip.bf1.yahoo.com:50513``     | ``hcat/axonitered-hcat.ygrid.vip.bf1.yahoo.com@YGRID.YAHOO.COM``      | Hadoop 2.x sandbox |
+----------+----------------------------------------------------------------+-----------------------------------------------------------------------+--------------------+
| BB       | ``thrift://bassniumblue-hcat-v0.ygrid.vip.gq1.yahoo.com:50513``| ``hcat/bassniumblue-hcat-v0.ygrid.vip.gq1.yahoo.com@YGRID.YAHOO.COM`` |                    |
+----------+----------------------------------------------------------------+-----------------------------------------------------------------------+--------------------+
| BR       | ``thrift://bassniumred-hcat.ygrid.vip.bf1.yahoo.com:50513``    | ``hcat/bassniumred-hcat.ygrid.vip.bf1.yahoo.com@YGRID.YAHOO.COM``     |                    |
+----------+----------------------------------------------------------------+-----------------------------------------------------------------------+--------------------+
| BT       | ``thrift://bassniumtan-hcat.ygrid.vip.ne1.yahoo.com:50513``    | ``hcat/bassniumtan-hcat.ygrid.vip.ne1.yahoo.com@YGRID.YAHOO.COM``     |                    |
+----------+----------------------------------------------------------------+-----------------------------------------------------------------------+--------------------+
| DB       | ``thrift://dilithiumblue-hcat.ygrid.vip.gq1.yahoo.com:50513``  | ``hcat/dilithiumblue-hcat.ygrid.vip.gq1.yahoo.com@YGRID.YAHOO.COM``   |                    |
+----------+----------------------------------------------------------------+-----------------------------------------------------------------------+--------------------+
| DR       | ``thrift://dilithiumred-hcat.ygrid.vip.bf1.yahoo.com:50513``   | ``hcat/dilithiumred-hcat.ygrid.vip.bf1.yahoo.com@YGRID.YAHOO.COM``    |                    |
+----------+----------------------------------------------------------------+-----------------------------------------------------------------------+--------------------+
| JB       | ``thrift://jetblue-hcat.ygrid.vip.gq1.yahoo.com:50513``        | ``hcat/jetblue-hcat.ygrid.vip.gq1.yahoo.com@YGRID.YAHOO.COM``         |                    |
+----------+----------------------------------------------------------------+-----------------------------------------------------------------------+--------------------+
| KR       | ``thrift://kryptonitered-hcat.ygrid.vip.bf1.yahoo.com:50513``  | ``hcat/kryptonitered-hcat.ygrid.vip.bf1.yahoo.com@YGRID.YAHOO.COM``   | Hadoop sandbox     |
+----------+----------------------------------------------------------------+-----------------------------------------------------------------------+--------------------+
| MR       | ``thrift://mithrilred-hcat.ygrid.vip.bf1.yahoo.com:50513``     | ``hcat/mithrilred-hcat.ygrid.vip.bf1.yahoo.com@YGRID.YAHOO.COM``      |                    |
+----------+----------------------------------------------------------------+-----------------------------------------------------------------------+--------------------+
| OB       | ``thrift://oxiumblue-hcat.ygrid.vip.gq1.yahoo.com:50513``      | ``hcat/oxiumblue-hcat.ygrid.vip.gq1.yahoo.com@YGRID.YAHOO.COM``       |                    |
+----------+----------------------------------------------------------------+-----------------------------------------------------------------------+--------------------+
| PB       | ``thrift://phazonblue-hcat.ygrid.vip.gq1.yahoo.com:50513``     | ``hcat/phazonblue-hcat.ygrid.vip.gq1.yahoo.com@YGRID.YAHOO.COM``      |                    |
+----------+----------------------------------------------------------------+-----------------------------------------------------------------------+--------------------+
| PR       | ``thrift://phazonred-hcat.ygrid.vip.bf1.yahoo.com:50513``      | ``hcat/phazonred-hcat.ygrid.vip.bf1.yahoo.com@YGRID.YAHOO.COM``       |                    |
+----------+----------------------------------------------------------------+-----------------------------------------------------------------------+--------------------+
| PT       | ``thrift://phazontan-hcat.ygrid.vip.ne1.yahoo.com:50513``      | ``hcat/phazontan-hcat.ygrid.vip.ne1.yahoo.com@YGRID.YAHOO.COM``       |                    |
+----------+----------------------------------------------------------------+-----------------------------------------------------------------------+--------------------+
| TT       | ``thrift://tiberiumtan-hcat.ygrid.vip.ne1.yahoo.com:50513``    | ``hcat/tiberiumtan-hcat.ygrid.vip.ne1.yahoo.com@YGRID.YAHOO.COM``     |                    |
+----------+----------------------------------------------------------------+-----------------------------------------------------------------------+--------------------+
| UB       | ``thrift://uraniumblue-hcat.ygrid.vip.gq1.yahoo.com:50513``    | ``hcat/uraniumblue-hcat.ygrid.vip.gq1.yahoo.com@YGRID.YAHOO.COM``     |                    |
+----------+----------------------------------------------------------------+-----------------------------------------------------------------------+--------------------+
| UT       | ``thrift://uraniumtan-hcat.ygrid.vip.ne1.yahoo.com:50513``     | ``hcat/uraniumtan-hcat.ygrid.vip.ne1.yahoo.com@YGRID.YAHOO.COM``      |                    |
+----------+----------------------------------------------------------------+-----------------------------------------------------------------------+--------------------+
| ZT       | ``thrift://zaniumtan-hcat.ygrid.vip.ne1.yahoo.com:50513``      | ``hcat/zaniumtan-hcat.ygrid.vip.ne1.yahoo.com@YGRID.YAHOO.COM``       |                    |
+----------+----------------------------------------------------------------+-----------------------------------------------------------------------+--------------------+
| Kessel   | ``thrift://kesselgq-hcat.gq.vcg.yahoo.com:50513``              | ``hcat/kesselgq-hcat-v0.vcg.vip.gq2.yahoo.com@VCG.OUROATH.COM``       | VCG                |
+----------+----------------------------------------------------------------+-----------------------------------------------------------------------+--------------------+
| Polaris  | ``thrift://polarisgq-hcat.gq.vcg.yahoo.com:50513``             | ``hcat/polarisgq-hcat-v0.vcg.vip.gq2.yahoo.com@VCG.OUROATH.COM``      | VCG                |
+----------+----------------------------------------------------------------+-----------------------------------------------------------------------+--------------------+

.. _gs_appendix-hs2_servers:

HiveServer2 Servers
-------------------
+----------+-----------------------------------------------------+----------+----------------------------------------------------------------------+--------------------+
| **Grid** | **Hostname**                                        | **Port** | **KerberosPrincipal**                                                | **Comments**       |
+==========+=====================================================+==========+======================================================================+====================+
| AR       | ``axonitered-hs2.ygrid.vip.bf1.yahoo.com``          | 50514    | ``hive/axonitered-hs2.ygrid.vip.bf1.yahoo.com@YGRID.YAHOO.COM``      | Hadoop 2.x sandbox |
+----------+-----------------------------------------------------+----------+----------------------------------------------------------------------+--------------------+
| BB       | ``bassniumblue-hs2-v0.ygrid.vip.gq1.yahoo.com``     | 50514    | ``hive/bassniumblue-hs2-v0.ygrid.vip.gq1.yahoo.com@YGRID.YAHOO.COM`` |                    |
+----------+-----------------------------------------------------+----------+----------------------------------------------------------------------+--------------------+
| BR       | ``bassniumred-hs2.ygrid.vip.bf1.yahoo.com``         | 50514    | ``hive/bassniumred-hs2.ygrid.vip.bf1.yahoo.com@YGRID.YAHOO.COM``     |                    |
+----------+-----------------------------------------------------+----------+----------------------------------------------------------------------+--------------------+
| BT       | ``bassniumtan-hs2.ygrid.vip.bf1.yahoo.com``         | 50514    | ``hive/bassniumtan-hs2.ygrid.vip.bf1.yahoo.com@YGRID.YAHOO.COM``     |                    |
+----------+-----------------------------------------------------+----------+----------------------------------------------------------------------+--------------------+
| DB       | ``dilithiumblue-hs2.ygrid.vip.gq1.yahoo.com``       | 50514    | ``hive/dilithiumblue-hs2.ygrid.vip.gq1.yahoo.com@YGRID.YAHOO.COM``   |                    |
+----------+-----------------------------------------------------+----------+----------------------------------------------------------------------+--------------------+
| DR       | ``dilithiumred-hs2.ygrid.vip.bf1.yahoo.com```       | 50514    | ``hive/dilithiumred-hs2.ygrid.vip.bf1.yahoo.com@YGRID.YAHOO.COM``    |                    |
+----------+-----------------------------------------------------+----------+----------------------------------------------------------------------+--------------------+
| JB       | ``jetblue-hs2.ygrid.vip.gq1.yahoo.com``             | 50514    | ``hive/jetblue-hs2.ygrid.vip.gq1.yahoo.com@YGRID.YAHOO.COM``         |                    |
+----------+-----------------------------------------------------+----------+----------------------------------------------------------------------+--------------------+
| KR       | ``kryptonitered-hs2.ygrid.vip.bf1.yahoo.com``       | 50514    | ``hive/kryptonitered-hs2.ygrid.vip.bf1.yahoo.com@YGRID.YAHOO.COM``   | Hadoop sandbox     |
+----------+-----------------------------------------------------+----------+----------------------------------------------------------------------+--------------------+
| MR       | ``mithrilred-hs2.ygrid.vip.bf1.yahoo.com``          | 50514    | ``hive/mithrilred-hs2.ygrid.vip.bf1.yahoo.com@YGRID.YAHOO.COM``      |                    |
+----------+-----------------------------------------------------+----------+----------------------------------------------------------------------+--------------------+
| OB       | ``oxiumblue-hs2.ygrid.vip.gq1.yahoo.com``           | 50514    | ``hive/oxiumblue-hs2.ygrid.vip.gq1.yahoo.com@YGRID.YAHOO.COM``       |                    |
+----------+-----------------------------------------------------+----------+----------------------------------------------------------------------+--------------------+
| PB       | ``phazonblue-hs2.ygrid.vip.gq1.yahoo.com``          | 50514    | ``hive/phazonblue-hs2.ygrid.vip.gq1.yahoo.com@YGRID.YAHOO.COM``      |                    |
+----------+-----------------------------------------------------+----------+----------------------------------------------------------------------+--------------------+
| PR       | ``phazonred-hs2.ygrid.vip.bf1.yahoo.com``           | 50514    | ``hive/phazonred-hs2.ygrid.vip.bf1.yahoo.com@YGRID.YAHOO.COM``       |                    |
+----------+-----------------------------------------------------+----------+----------------------------------------------------------------------+--------------------+
| PT       | ``phazontan-hs2.ygrid.vip.ne1.yahoo.com``           | 50514    | ``hive/phazontan-hs2.ygrid.vip.ne1.yahoo.com@YGRID.YAHOO.COM``       |                    |
+----------+-----------------------------------------------------+----------+----------------------------------------------------------------------+--------------------+
| TT       | ``tiberiumtan-hs2.ygrid.vip.ne1.yahoo.com``         | 50514    | ``hive/tiberiumtan-hs2.ygrid.vip.ne1.yahoo.com@YGRID.YAHOO.COM``     |                    |
+----------+-----------------------------------------------------+----------+----------------------------------------------------------------------+--------------------+
| UB       | ``uraniumblue-hs2.ygrid.vip.gq1.yahoo.com``         | 50514    | ``hive/uraniumblue-hs2.ygrid.vip.gq1.yahoo.com@YGRID.YAHOO.COM``     |                    |
+----------+-----------------------------------------------------+----------+----------------------------------------------------------------------+--------------------+
| UT       | ``uraniumtan-hs2.ygrid.vip.ne1.yahoo.com``          | 50514    | ``hive/uraniumtan-hs2.ygrid.vip.ne1.yahoo.com@YGRID.YAHOO.COM``      |                    |
+----------+-----------------------------------------------------+----------+----------------------------------------------------------------------+--------------------+
| ZT       | ``zaniumtan-hs2.ygrid.vip.ne1.yahoo.com``           | 50514    | ``hive/zaniumtan-hs2.ygrid.vip.ne1.yahoo.com@YGRID.YAHOO.COM``       |                    |
+----------+-----------------------------------------------------+----------+----------------------------------------------------------------------+--------------------+
| Kessel   | ``kesselgq-hs.gq.vcg.yahoo.com``                    | 4443     | ``HTTP/kesselgq-hs.gq.vcg.yahoo.com@VCG.OUROATH.COM``                | VCG                |
+----------+-----------------------------------------------------+----------+----------------------------------------------------------------------+--------------------+
| Polaris  | ``polarisgq-hs.gq.vcg.yahoo.com``                   | 4443     | ``HTTP/polarisgq-hs.gq.vcg.yahoo.com@VCG.OUROATH.COM``               | VCG                |
+----------+-----------------------------------------------------+----------+----------------------------------------------------------------------+--------------------+

.. _gs_appendix-hs2_servers_proposed:

(Future) HiveServer2 Servers (After Thrift/HTTPS rollout on YGRID)
------------------------------------------------------------------
+----------+----------------------------------------------+----------+----------------------------------------------------------------------+--------------------+
| **Grid** | **Hostname**                                 | **Port** | **Kerberos Principal**                                               | **Comments**       |
+==========+==============================================+==========+======================================================================+====================+
| AR       | ``axonitered-hs2.red.ygrid.yahoo.com``       | 4443     | ``hive/axonitered-hs2.red.ygrid.yahoo.com@YGRID.YAHOO.COM``          | Hadoop 2.x sandbox |
+----------+----------------------------------------------+----------+----------------------------------------------------------------------+--------------------+
| BB       | ``bassniumblue-hs2.blue.ygrid.yahoo.com``    | 4443     | ``hive/bassniumblue-hs2.blue.ygrid.yahoo.com@YGRID.YAHOO.COM``       |                    |
+----------+----------------------------------------------+----------+----------------------------------------------------------------------+--------------------+
| BR       | ``bassnniumred-hs2.red.ygrid.yahoo.com``     | 4443     | ``bassnniumred-hs2.red.ygrid.yahoo.com@YGRID.YAHOO.COM``             |                    |
+----------+----------------------------------------------+----------+----------------------------------------------------------------------+--------------------+
| BT       | ``bassniumtan-hs2.tan.ygrid.yahoo.com``      | 4443     | ``hive/bassniumtan-hs2.tan.ygrid.yahoo.com@YGRID.YAHOO.COM``         |                    |
+----------+----------------------------------------------+----------+----------------------------------------------------------------------+--------------------+
| DB       | ``dilithiumblue-hs2.blue.ygrid.yahoo.com``   | 4443     | ``hive/dilithiumblue-hs2.blue.ygrid.yahoo.com@YGRID.YAHOO.COM``      |                    |
+----------+----------------------------------------------+----------+----------------------------------------------------------------------+--------------------+
| DR       | ``dilithiumred-hs2.red.ygrid.yahoo.com``     | 4443     | ``hive/dilithiumred-hs2.red.ygrid.yahoo.com@YGRID.YAHOO.COM``        |                    |
+----------+----------------------------------------------+----------+----------------------------------------------------------------------+--------------------+
| JB       | ``jetblue-hs2.blue.ygrid.yahoo.com``         | 4443     | ``hive/jetblue-hs2.blue.ygrid.yahoo.com@YGRID.YAHOO.COM``            |                    |
+----------+----------------------------------------------+----------+----------------------------------------------------------------------+--------------------+
| KR       | ``kryptonitered-hs2.red.ygrid.yahoo.com``    | 4443     | ``hive/kryptonitered-hs2.red.ygrid.yahoo.com@YGRID.YAHOO.COM``       | Hadoop sandbox     |
+----------+----------------------------------------------+----------+----------------------------------------------------------------------+--------------------+
| MR       | ``mithrilred-hs2.red.ygrid.yahoo.com``       | 4443     | ``hive/mithrilred-hs2.red.ygrid.yahoo.com@YGRID.YAHOO.COM``          |                    |
+----------+----------------------------------------------+----------+----------------------------------------------------------------------+--------------------+
| OB       | ``oxiumblue-hs2.blue.ygrid.yahoo.com``       | 4443     | ``hive/oxiumblue-hs2.blue.ygrid.yahoo.com``                          |                    |
+----------+----------------------------------------------+----------+----------------------------------------------------------------------+--------------------+
| PB       | ``phazonblue-hs2.blue.ygrid.yahoo.com``      | 4443     | ``hive/phazonblue-hs2.blue.ygrid.yahoo.com@YGRID.YAHOO.COM``         |                    |
+----------+----------------------------------------------+----------+----------------------------------------------------------------------+--------------------+
| PR       | ``phazonred-hs2.red.ygrid.yahoo.com``        | 4443     | ``hive/phazonred-hs2.red.ygrid.yahoo.com@YGRID.YAHOO.COM``           |                    |
+----------+----------------------------------------------+----------+----------------------------------------------------------------------+--------------------+
| PT       | ``phazontan-hs2.tan.ygrid.yahoo.com``        | 4443     | ``hive/phazontan-hs2.tan.ygrid.yahoo.com@YGRID.YAHOO.COM``           |                    |
+----------+----------------------------------------------+----------+----------------------------------------------------------------------+--------------------+
| TT       | ``tiberiumtan-hs2.tan.ygrid.yahoo.com``      | 4443     | ``hive/tiberiumtan-hs2.tan.ygrid.yahoo.com@YGRID.YAHOO.COM``         |                    |
+----------+----------------------------------------------+----------+----------------------------------------------------------------------+--------------------+
| UB       | ``uraniumblue-hs2.blue.ygrid.yahoo.com``     | 4443     | ``hive/uraniumblue-hs2.blue.ygrid.yahoo.com@YGRID.YAHOO.COM``        |                    |
+----------+----------------------------------------------+----------+----------------------------------------------------------------------+--------------------+
| UT       | ``uraniumtan-hs2.tan.ygrid.yahoo.com``       | 4443     | ``hive/uraniumtan-hs2.tan.ygrid.yahoo.com@YGRID.YAHOO.COM``          |                    |
+----------+----------------------------------------------+----------+----------------------------------------------------------------------+--------------------+
| ZT       | ``zaniumtan-hs2.tan.ygrid.yahoo.com``        | 4443     | ``hive/zaniumtan-hs2.tan.ygrid.yahoo.com@YGRID.YAHOO.COM``           |                    |
+----------+----------------------------------------------+----------+----------------------------------------------------------------------+--------------------+
| Kessel   | ``kesselgq-hs.gq.vcg.yahoo.com``             | 4443      | ``HTTP/kesselgq-hs.gq.vcg.yahoo.com@VCG.OUROATH.COM``               | VCG                |
+----------+----------------------------------------------+----------+----------------------------------------------------------------------+--------------------+
| Polaris  | ``polarisgq-hs.gq.vcg.yahoo.com``            | 4443     | ``HTTP/polarisgq-hs.gq.vcg.yahoo.com@VCG.OUROATH.COM``               | VCG                |
+----------+----------------------------------------------+----------+----------------------------------------------------------------------+--------------------+

.. _gs_appendix-generate-role-certs:

Athenz User/Role certificates, and Java KeyStores
=================================================

User/role certificates may be fetched as per `GridOps' documentation <https://docs.google.com/document/d/1fUziPmsB-QALJtqQ6QZ9xf18n6mLOqRHasR9Ru7hXMg/edit>`_. The following is a short example, for illustration:

#. Fetch user-certificate: ::

    $ athenz-user-cert
    Touch Yubikey if flashing

    $ ls -al ~/.athenz/{cert,key}
    -rw------- 1 mithunr users 1659 Sep 26 06:11 /homes/mithunr/.athenz/cert
    -r-------- 1 mithunr users 1679 Sep 25 17:50 /homes/mithunr/.athenz/key

#. Use user-certificate to then fetch appropriate role-certificate. For ``YGRID`` clusters, use ``griduser`` as domain. For ``VCG`` clusters, use ``vcg.user``.
The following command fetches the ``YGRID`` (i.e. ``griduser``) role-cert for ``uid.mithunr`` and stores it in ``/homes/mithunr/.athenz/griduser.role.uid.mithunr.pem`` in PEM format: ::

    $ zts-rolecert -svc-key-file ~/.athenz/key -svc-cert-file ~/.athenz/cert -zts https://zts.athens.yahoo.com:4443/zts/v1 \
    -dns-domain zts.yahoo.cloud -role-domain griduser -role-name uid.mithunr -role-cert-file griduser.role.uid.mithunr.pem

#. To use the role-certificate from a Java application (such as Hive Beeline), the certificate usually needs conversion to Java KeyStore (JKS) format.
As a first step, the certificate would need conversion to PKCS12 format, as follows: ::

    $ openssl pkcs12 -export -inkey key -in griduser.role.uid.mithunr.pem -out griduser.role.uid.mithunr.pkcs12 -password pass:changeit

#. Finally, the resultant ``griduser.role.uid.mithunr.pkcs12`` can be imported into a JKS file, using ``keytool``: ::

   $ keytool -importkeystore -srckeystore griduser.role.uid.mithunr.pkcs12 -srcstoretype PKCS12 -srcstorepass changeit -destkeystore griduser.role.uid.mithunr.jks -deststorepass changeit -noprompt

#. The resultant ``griduser.role.uid.mithunr.jks`` Java KeyStore can now be used with Java programs, such as with :ref:`Hive Beeline <beeline_jdbc_https_x509>`.

