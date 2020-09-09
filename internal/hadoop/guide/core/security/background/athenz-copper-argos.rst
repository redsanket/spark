.. _security_background_copper_argos:

*******************
Athenz Copper Argos
*******************

.. admonition:: Reading...
   :class: readingbox

   Details of the protocol and flow:

   .. include:: /common/security/athenz/copper-argos-reading-resources.rst

Overview
========

Copper Argos is Athenz generalized model for service providers to launch other
service identities in an authorized way through a callback-based verification
model. It provides a secure and generic way for service providers to launch
other authorized services with Athenz issued service identity X.509
certificates. These certificates provide a more secure solution than private key
generated NTokens since they can be used to communicate with other Athenz
enabled services without exposing their identity tokens.

As described in :numref:`security_background_athenz`, Athenz introduced the
concept of "`service identities`" where the service administrator would generate
a public/private RSA or EC key pair, register the public key in Athenz, and then
it could issue authentication tokens (*NTokens*) signed by its private key that
could be presented to Athenz to authenticate the service.

.. topic:: Why Copper-Argos Exists?

  While Athenz' service identity works well when services talk to Athenz
  services, it is not secure for server-to-server communication. A tenant
  service must not present its `NToken` to a provider service since, if
  compromised, the provider service may act as the tenant service while
  communicating with other services. Athenz solution was to use authorization
  role tokens (`ZToken`) that were scoped for a specific role in the
  requested domain.

Copper Argos was designed and implemented that would allow the service to
be identified by its *X.509 certificate* issued by Athenz:

* Athenz would integrate with a Certificate Signer Daemon that would store its
  private key in HSM (e.g. AWS CloudHSM)
* An :term:`SIA` running on an instance would generate a key pair on the
  instance itself, generate a :term:`CSR` and send along with its authentication
  details to :term:`ZTS` Server to request a certificate for the service.
* Once the request is authorized and validated, :term:`ZTS` would contact
  *Certificate Signer Daemon* to mint a certificate for the instance.
  It would be valid for 30 days and the SIA running on the instance will be
  responsible to refresh the certificate daily.
* The service running on host can use the generated private key and the
  X.509 certificate it received from ZTS to identify itself to other services
  running within the organization.

This model provides a significant improvement over the use of NTokens since the
services now have a secure way of identifying themselves without the use of role
tokens.



The high level requirements for Copper Argos are as follows:

#. Once authorized as a service launcher in Athenz, the Provider will launch the
   `Service` by providing it with a signed identity document.
#. SIA installed on the instance during bootstrap process or
   available as part of the base image that the service is deployed on will
   create a public/private key pair, generate a :term:`CSR`
   and submit the identity document along with its CSR to Athenz ZTS
   Service.
#. Athenz ZTS Service would carry out 2 authorization checks:

   #. It verifies that the `Provider` is authorized to launch services. This
      prohibits any service act like a provider and attempt to launch other
      services.
   #. It verifies that the `Service` being launched has authorized
      the `Provider` as its launcher. This prohibits any authorized `Provider` to
      launch any other service without explicit authorization from the tenant
      service administrator.

#. Athenz ZTS contacts the `Provider` to validate the signed instance identity
   document.
#. Once verified, ZTS contacts "*Certificate Signer Daemon*" to generate a
   X.509 Certificate valid for 30 days for the given :term:`CSR` and returns to
   instance.
#. :term:`SIA` is responsible for contacting ZTS daily to refresh the service
   X.509 certificate.

Provider Service Registration
-----------------------------

The `Provider` must retrieve a :term:`TLS` certificate for its own service
from Athenz and use that as its Server Certificate.

When ZTS contacts Provider Service to validate an instance document, it will
require TLS authentication and verify that the certificate the service used for
establish secure communication was issued for the expected provider service by
ZTS itself.

The `provider` will also specify a unique registered "*dns suffix*" that would
be combined with the service identity name and included as a ``dnsName``
attribute value in the X.509 certificate.

For example, if an instance deployed is going to have an Athenz identity
of "`sports.api`" and the Provider’s dns suffix is "`sports.athenz.cloud`", then
the `dnsName` attribute in the generated X.509 certificate would be
"`api.sports.sports.athenz.cloud`".

.. figure:: https://yahoo.github.io/athenz/images/dev_provider_service_registration.png
   :alt: Athenz Provider Service Registration
   :width: 100%
   :align: center
   
   Athenz Provider Service Registration



Instance Register Request
--------------------------

The tenant domain administrator must authorize a specific provider service to
launch its services.

.. sidebar:: Must Read

   Steps of the process in Yahoo Github pages -
   `Instance Register Request <https://yahoo.github.io/athenz/copper_argos_dev/#instance-register-request>`_

The provider is expected to configure separate launcher services for each data
center and/or region it supports. This adds flexibility to authorization.
For example:

* "`sys.auth.aws.us-west`" - launch its service in specific data center/region.
* "`sys.auth.aws.*`" - all supported data centers/regions

.. note::
   Although each provider is strongly recommended to represent their instance
   documents as **signed JWT (Json Web Tokens)**, ZTS treats it as an
   opaque string and relies on the registered provider service callback verifier
   to validate the document.

.. figure:: https://yahoo.github.io/athenz/images/instance_register_request.png
   :alt: Instance Register Request
   :width: 100%
   :align: center
   
   Instance Register Request

ZTS does impose specific requirements on the X.509 CSR generated by :term:`SIA`:

#. The `Subject CN` in the :term:`CSR` must be the Athenz service identity name.
   (e.g. "`sports.api`" - api service in sports domain)
#. The :term:`CSR` must include a `SAN dnsName` attribute with the format:
   ``<service-with-domain>.<provider-dnsname-suffix>.``.
   The provider-dnsname-suffix must be provided by the provider service
   to the SIA agent.
#. The provider must specify a unique identifier for the instance within the
   provider’s namespace (this could be combination of multiple strings -
   e.g. "`instance-id.pod-id.cluster-id`") and this information must be
   included in the CSR as another `SAN dnsName` attribute with the format:
   “`.instanceid.athenz.`”.
#. The ZTS server will use this information along with the issued TLS Certificate
   Serial number to revoke specific instances if they’re compromised.
   ZTS Server will carry out the following authorization and verification checks
   before contacting provider service.

.. figure:: https://yahoo.github.io/athenz/images/dev_service_identity_register.png
   :alt: Athenz Provider Service Registration
   :width: 100%
   :align: center
   
   Athenz Service Identity Bootstrap

Instance Refresh Request
------------------------

During instance refresh request, ZTS Server again contacts provider service for
verification of the instance. This is necessary in case the credentials have
been compromised and the original instance is no longer running and will allow
the provider to verify that the instance is indeed still running. |br|
ZTS will also verify that the connection is using the previous TLS certificate
issued for the service and provider and the authorization policies are still in
place.

.. figure:: https://yahoo.github.io/athenz/images/instance_refresh_request.png
   :alt: Instance Refresh Request
   :width: 100%
   :align: center

   Instance Refresh Request

.. sidebar:: Must Read

   Steps of the process in Yahoo Github pages -
   `Instance Refresh Request <https://yahoo.github.io/athenz/copper_argos_dev/#instance-refresh-request>`_

.. rubric:: A requirement of Copper Argos 

is that the CSR that the SIA generates must include a unique id that the
provider has assigned to that instance in the `X509 v3 SAN Extension dnsName`
field.  Since each certificate is uniquely identified by its `Serial Number`
generated by Athenz CA, Athenz ZTS can use that information along with instance
id to maintain list of all issued certificates for instances with their serial
numbers, detect compromised certificate refresh requests and revoke specific
hosts from the refresh requests.


Requests Format (:term:`RDL`)
=============================


.. literalinclude:: /resources/security/copper-argos/rdls/instance-register-request.rdl
    :language: c
    :linenos:
    :name: lst-copper-argos-register-req
    :caption: ``InstanceRegisterInformation`` is the information a
              provider-hosted booting instance must provide to Athenz to authenticate
    

.. literalinclude:: /resources/security/copper-argos/rdls/instance-identity-response.rdl
    :language: c
    :linenos:
    :name: lst-copper-argos-identity-resp
    :caption: ``InstanceIdentity`` is returned on successful attestation. It
            includes the certifictates, tokens, and any additional provider-specific
            derived attributes.

.. literalinclude:: /resources/security/copper-argos/rdls/instance-refresh-request.rdl
    :language: c
    :linenos:
    :name: lst-copper-argos-refresh-req
    :caption: ``InstanceRefreshInformation`` is the information a
            provider-hosted booting instance must provide to Athenz to refresh
            the TLS certificate it was issued initially. The request must be
            done using the previously issued client TLS certificate.

.. literalinclude:: /resources/security/copper-argos/rdls/instance-revoke-request.rdl
    :linenos:
    :language: none
    :name: lst-copper-argos-revoke-req
    :caption: Instance Revoke Request

.. literalinclude:: /resources/security/copper-argos/rdls/instance-provider.rdl
    :language: c
    :linenos:
    :name: lst-copper-argos-provider
    :caption: the RDL that any provider that is authorized to create instances
              through Athenz must implement. This interface must be available
              over HTTPS only.

.. literalinclude:: /resources/security/copper-argos/rdls/instance-provider-response.rdl
    :language: c
    :linenos:
    :name: lst-copper-argos-provider-response
    :caption: ``InstanceConfirmation``: response to ZTS server


Provider Client Implementation
==============================

.. admonition:: Reading...
   :class: readingbox

   Check Athenz Github - `copper_argos_dev: Sample Implementation and Setup Tasks <https://yahoo.github.io/athenz/copper_argos_dev/#sample-implementation-and-setup-tasks>`_

* Provider must implement a client that will communicate with ZTS and execute
  Instance Register and Instance Refresh requests.
* When bootstrapping the tenant instance, the Provider service must pass to the
  client the following details:

  * the provider service name (e.g. openstack.cluster1)
  * its dns suffix (e.g. cluster1.ostk.athenz.cloud)
  * signed instance identity document

* The client will generate a :term:`CSR` based on the details it received from
  its provider service, include the CSR in the `InstanceRegisterInformation`
  (:numref:`lst-copper-argos-register-req`) object and post it to `ZTS`.

* The client will be responsible for storing the certificate used from ZTS on
  the local host and use it for its subsequent requests to ZTS to refresh the
  certificate.
