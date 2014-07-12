=============
Authorization
=============

Supported Authorization Methods
===============================

- **HTTP** - Using HTTP Authentication or with a Custom Java Servlet Filter.
- **Thrift** - Kerberos (Possibly through a forwarded TGT)
- **ZooKeeper** - Kerberos for system processes (Because there is a keytab available) a 
  shared secret for worker processes with MD5SUM in ZK
- **File System** - OS user + FS permissions. Some processes on the same system communicate through files
- **Worker to Worker** - Can use encryption with shared secret, but we really need to add in SASL Auth.
- **External Services (like HBase)** - TBD: Sorry it is up to you (Sort of …) 


Credentials Push (Authenticating with External Services)
========================================================

A set of APIs and plugins that allow credentials to securely be delivered and renewed.

- **ICredentialsListener** - Using HTTP Authentication or with a Custom Java Servlet Filter.
- **IAutoCredentials** - Kerberos (Possibly through a forwarded TGT).
- **ICredentialsRenewer** - Kerberos for system processes (Because there is a 
  keytab available) a shared secret for worker processes with MD5SUM in ZK.
- **storm upload_credentials** - OS user + FS permissions. Some processes on the 
  same system communicate through files.
- **AutoTGT** - Can use encryption with shared secret, but we really need to add in SASL Auth.

Plug-In API
===========

A plugin API has also been added to block unwanted operations, along with some reasonable implementations.

For Example SimpleACLAuthorizer for Nimbus.

- Can configure Administrators that can do anything.
- Users that the supervisors are running as.
- Topology can also configure who is allowed to kill or rebalance it.

DRPC Authorization is still being worked on, but should be done soon.



