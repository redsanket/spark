==========================
Storm Registry Service API
==========================

.. Status: first draft. 

Overview
========

The Registry Service is a simple Web service designed to allow clients to discover 
services running on a cloud, similar to Hadoop or Storm, and securely connect to 
them (as the registry is a trusted 3rd party). There are really two concepts involved here.

The first is that of a virtual host. A virtual host represents a service. It is 
called a virtual host because it is intended to be compatible with DNS or other 
name services and might appear to end user applications as a logical host similar 
to a virtual IP address.

The second concept is that of a server. A server provides the service that a 
virtual host represents and places itself in the registry so that clients can 
find it.

Security
========

Authentication With Registry
----------------------------

Authentication for the Registry Service is pluggable. When adding a virtual 
host, a list of owners is given with it. This list of owners determines who 
is allowed to modify the virtual host and who is allowed to see private credential 
data about the virtual host. Owners are optionally prefixed with a type to indicate 
exactly how the user should authenticate. By default, an owner without a type 
corresponds to a user.

At Yahoo, we use YCA for this Authentication: both YCA v1 and v2 are supported. 
YCA owners must be prefixed with ``"yca:"``. Any API that requires authorization will 
need to have an appropriate YCA header set, and, in the case of YCA v2, will need 
to go through the appropriate proxy servers.

.. _registry_service_api-authenticating:

Authenticating Services
-----------------------

Because the Registry Service is polled to find out where server instances are 
running, clients should validate they are talking to a real instance of the 
service before sending any real data. The Registry Service provides a pluggable 
mechanism to support this.

These plugins are controlled by the ``securityData`` object in the ``virtualHost``. 
The keys of all objects in ``securityData`` are the names of the plugins. When 
adding in a new virtual host, the object is used as configuration for the plugin. 
When the virtual host is read, it is used to return information about that plugin. 
Typically, a user must be an owner of the virtual host to see all of the data 
in these sections.

Self-Signed SSL
---------------

The service comes with the ability to use self-signed SSL certificates. When a 
virtual host is created a new public/private key pair is generated and stored in 
a ``pkcs12`` keystore. The corresponding RFC compliant certificate is also generated.

When generating a virtual host, the following options can be passed:

- **dname** - the dname to use with the certificate. This is usually in the for 
  of ``"CN=<Something>, OU=<Something>..."``, defaults to ``"CN=apache, OU=yarn, O=registry"``.
- **alias** - the name/alias the public/private keys will be stored under in the 
  keystore. The defaults is ``"selfsigned"``.
- **password** - the password used to encrypt the keystore. Defaults to a randomly 
  generated password.
- **validity** - an integer indicating the number of days that this SSL certificate 
  should be good for. The default value is 365*2 days.

Examples
########

General
*******

.. code-block:: javascript

   {
      "virtualHost":{
         "name":"ssltest.yahoo.com", 
         "owner":["yca:some-role"],
         "securityData":{
             "SelfSignedSSL":{
                 "alias": "ssltest.yahoo.com",
                 "dname": "CN=ssltest.yahoo.com, OU=Unknown, O=Unknown",
                 "validity": 1825
             }
         }
      }
   }

When reading data, two possible data sets may be returned: either 
the client is authenticated and an owner of ``virtualHost``, in which case all data 
will be returned, or it is not, in which case only public information will 
be returned.

Return Values
^^^^^^^^^^^^^

- **password** - (PRIVATE) the password used to encrypt the ``pkcs12`` key store.
- **alias** - (PRIVATE) where in the key store the public/private key pairs are stored.
- **pkcs12** - (PRIVATE) a Base64-encoded PKCS12 keystore.
- **cert** - (PUBLIC) a RFC compatible base64-encoded certificate.

Private
*******

.. code-block:: javascript

   "virtualHost":{
       "name":"ssltest.yahoo.com", 
       "owner":["yca:some-role"],
       "securityData":{
           "SelfSignedSSL":{
               "password":"0d86eaad-fc9b-4178-bb4c-f268fd72ed18",
               "alias":"ssltest.yahoo.com",
               "pkcs12":"MIIJsAIBAzCCCWoGCSqGSIb3...",
               "cert":"-----BEGIN CERTIFICATE-----\nMIIDBTCCAe2..."
           }
       }
   }

Public
******

.. code-block:: javascript

   "virtualHost":{
       "name":"ssltest.yahoo.com", 
       "owner":["yca:some-role"],
       "securityData":{
           "SelfSignedSSL":{
               "cert":"-----BEGIN CERTIFICATE-----\nMIIDBTCCAe2..."
           }
       }
   }

There is a special endpoint for self-signed SSL to just fetch the public certificate: 
``/virtualHost/{virtualHost}/SelfSignedSSL.cert``

Bypassing the Registry
----------------------

If you are doing simple integration tests on a single node cluster, it can be
cumbersome to use the Registry Service. If you configure the registry URL to be ``null`` 
or an empty string, it will disable all calls to the registry server. Be careful 
when doing this though, as it can be difficult in production to think it is 
working, but really it is not talking to the registry at all. If you do this be 
sure to set it up to use HTTP, and not HTTPS, because the spout will try to pull 
the private key out of the Registry Service and fail.


Passing a SSL Certificate to cURL
---------------------------------

To get the SSL cert to pass to cURL, you can either use the option ``-k`` to let it accept any 
type of certificate, or you can obtain the certificate from the Registry Service by calling
``curl -Ss http://<registry>:<port>/registry/v1/virtualHost/<virtualHostName>/SelfSignedSSL.cert > my.cert``.
And then you can use the ``-E`` option to tell cURL to accept the certificate.



APIs
====

All of the APIs are prefixed with ``http://<host>:<port>/registry/v1/``.
So, for example, to call the ``status`` API, you would make
an HTTP request to the endpoint ``http://<host>:<port>/registry/v1/status``.

status
------

GET
###

You can check the status of the service. If the service is still up, the values 
``200`` and ``OK`` is returned.

admin/virtualHostRecrypt
------------------------

GET
###

Updates the encryption on all virtual hosts to use the newest keys. This is 
intended to be done by administrators after they role a secret key, but before the old 
key expires.

Example Response
****************

.. code-block:: javascript

   {
       "result": {
           "test.reg.yahoo.com":"OK"
       }
   }

virtualHost
-----------

GET
###

Lists all of the currently known virtual hosts.

Example Response
****************

.. code-block:: javascript

   {
       "virtualHostNames":["test.reg.yahoo.com"]
   }


POST
####

Adds a new virtual host.

Parameters
**********

- **name** - (REQUIRED) the name of virtual host, should conform to DNS name 
  semantics.
- **scheme** - (OPTIONAL) what is the protocol/scheme that should be used to access 
  this service.
- **port** - (OPTIONAL) integer port number for this service (currently there is 
  no enforcement on this to be unique, but we expect to add that in the future).
- **timeout** - (OPTIONAL) integer number of seconds a server can go without 
  heart-beating into the registry and still be considered alive. The default 
  value is 600.
- **owner** - a list of the owners of this virtual host (see below) .
- **securityData** - see :ref:`Authenticating Services <registry_service_api-authenticating>` for
  details.

If the ``securityData`` parameter is not specified or has an empty value,
the virtual host is considered to be insecure and having the ``owner`` parameter 
is not required. Otherwise, it is considered to be a secure virtual host and the 
``owner`` parameter is required.

If the ``owner`` parameter is specified, the ``virtualHost`` will only be added 
if the user adding the virtual host is an administrator or one of the owners.

Example Response
****************

.. code-block:: javascript

   {
       "virtualHost":{
           "name":"test.reg.yahoo.com", 
           "owner":["yca:some-role"],
           "securityData":{
               "SelfSignedSSL":{
                   "alias": "ssltest.yahoo.com",
                   "dname": "CN=ssltest.yahoo.com, OU=Unknown, O=Unknown",
                   "validity": 1825
               }
           }
       }
   }

virtualHost/{virtualHost}
-------------------------

DELETE
######

Deletes the virtual host. The user must be an administrator or one of the owners.

GET
###

Returns the details of the virtual hosts. If the user is an owner of the virtual host, the
full secure information will be returned. If the user is not, only public information 
will be returned.

Example Response
****************

.. code-block:: javascript

   "virtualHost":{
       "name":"ssltest.yahoo.com", 
       "owner":["yca:some-role"],
       "securityData":{
           "SelfSignedSSL":{
               "cert":"-----BEGIN CERTIFICATE-----\nMIIDBTCCAe2..."
           }
       }
   }

virtualHost/{virtualHost}/server
--------------------------------

GET
###

Lists all of the servers associated with this virtual host.

Example Response
****************

.. code-block:: javascript
 
   {
       "server":[
           {
               "serverId":"server1",
               "host":"myhost",
               "hb":1386623124847
           }
       ]
   }

virtualHost/{virtualHost}/server/{server}
-----------------------------------------

PUT
###

Adds a server instance or update an existing one. If the virtual host is secure 
only an owner can call this.

The only required field is the host the service is running on. An optional port 
may also be given.

Example Request
***************

.. code-block:: javascript

   {
       "server":{
           "host": "myhost"
       }
   }


GET
###

Gets the current information about this server.

The ``hb`` property in the returned response is the UNIX time of when the server last registered.

Example Response
****************

.. code-block:: javascript

   {
       "server":{
           "serverId":"server1",
           "host":"myhost",
           "hb":1386623124847
       }
   }

DELETE
######

Deletes the server instance.

virtualHost/{virtualHost}/ext/yahoo/yfor_config
-----------------------------------------------

GET
###

Returns a ``yfor`` configuration for this given virtual host. By default it is a 
very minimal configuration, but any query parameter in the URL is added to the 
returned configuration.

Example Response
****************

::

    name test
    host host0.yahoo.com
    host host1.yahoo.com
    host host2.yahoo.com
    check-type none
    mode all-active
    ttl 30000
