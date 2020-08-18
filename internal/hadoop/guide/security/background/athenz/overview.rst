.. _security_background_athenz:

********
Overview
********


Definition
==========

Athenz is a set of services and libraries supporting service authentication and
role-based authorization (RBAC) for provisioning and configuration (centralized
authorization) use cases as well as serving/runtime (decentralized
authorization) use cases. |br|
Athenz authorization system utilizes
`x.509 certificates <https://en.wikipedia.org/wiki/X.509>`_ and two types of
tokens:

* Principal Tokens (N-Tokens); and
* RoleTokens (Z-Tokens).

The use of x.509 certificates is strongly recommended over tokens.

The name "Athenz" is derived from "AuthNZ" (N for authentication and Z for
authorization).

.. topic:: How Athenz Work?

   Athenz introduced the concept of "`service identities`" where the service
   administrator would generate a public/private RSA or EC key pair, register the
   public key in Athenz, and then it could issue authentication tokens (*NTokens*)
   signed by its private key that could be presented to Athenz to authenticate the
   service.


Service Authentication
----------------------

Athenz provides secure identity in the form of short lived X.509 certificate for
every workload or service deployed in private (e.g. Openstack, K8S, Screwdriver)
or public cloud (e.g. AWS EC2, ECS, Fargate, Lambda).

Using these X.509 certificates clients and services establish secure connections
and through :term:`mutual TLS` authentication verify each other's identity.

The service identity certificates are valid for 30 days only and the service
identity agents (:term:`SIA`) part of those frameworks automatically refresh them
daily. Note that `A service identity` could represent a command, job, daemon,
workflow, as well as both an application client and an application service.


Role-Based Authorization (RBAC)
-------------------------------

Once the client is authenticated with its x.509 certificate, the service can
then check if the given client is authorized to carry out the requested action.
Athenz provides fine-grained :term:`RBAC`. It also provides a delegated management
model that supports multi-tenant and self-service concepts. 

AWS Temporary Credentials Support
---------------------------------

When working with AWS, Athenz provides support to access AWS services from
on-prem services with using AWS temporary credentials rather than static
credentials. Athenz :term:`ZTS` server can be used to request AWS temporary credentials
for configured AWS IAM roles.


Concepts
========

.. _fig-security-background-athenz-data-model:

.. figure:: /images/security/athenz/athenz_data_model_new.png
  :alt: Athenz Data Flow
  :width: 100%
  :align: center

.. sidebar:: Reading...

  Having a firm grasp on some fundamental concepts will help you understand the
  Athenz architecture, service authentication support, the flow for both
  centralized and decentralized authorization, and how to set up role-based
  authorization. Full details are in internal Athenz -
  `Guide: Athenz Concepts <https://git.vzbuilders.com/pages/athens/athenz-guide/concepts/>`_ 
  
.. _security_background_athenz_concepts_domains:

Domains
-------

Only system administrators can create and destroy Athenz top level domains.
These top level :term:`domains` are only created for registered products in
Verizon Media Product Master.

* Each domain is assigned users in an administrative :term:`role`.
* Each domain in Athenz has a special role and a policy called "*admin*".
* The members of the role `admin` have full access over all :term:`resources`
  defined in the :term:`domains` thus are called domain administrators.
* For :term:`subdomains`:

  * Each environment has a separate subdomain. For example, "`media.news`"
    should have three separate environments:
    "`media.news.prod`", "`media.news.stage`" and "`media.news.dev`".
  * Each subdomains will have its own set of administrators.
    For example, the development team members can be set as admins in the
    "`media.news.dev`" domain without having write access in the
    "`media.news.prod`" domain where they could possibly affect their production environment.

In addition to subdomains, Athenz provides personal domains in the form
of "`home.<okta-id>`". Every engineer can create their own personal
domain in the Athenz UI. This can be used for testing.

Resources
---------

When referring to :term:`resources` for authorization, a global naming system
needs to be introduced to handle names in namespaces. A Resource Name
(`YRN`) is defined as follows:

.. code-block:: bash

   {Domain}:{Resource}

The *Domain* is the namespace mentioned in
:numref:`security_background_athenz_concepts_domains`, and the *Resource*
is an
entity path (delimited by `periods`) within that namespace. The two are
often used together to form a "`short resource name`". When using the auto
tenancy functionality provided within Athenz, it uses a predefined
format to name resources:

.. code-block:: bash

   <provider-domain>:service.<provider-service>.tenant.<tenant-domain>.res_group.<resource-group>

For example, if the "`media.news`" tenant domain provisions a table
called "`opinions`" in a `sherpa` service, the name for the table may look
like this:


.. code-block:: bash

   sherpa:service.storage.tenant.media.news.res_group.opinions

   <provider-domain> -> sherpa
   <provider-service> -> storage
   <tenant-domain> -> media.news
   <resource-group> -> opinions

For :term:`CKMS`, if "`media.news`" has a keygroup called "`finance.portfolio`"
then the resource would look like this:

.. code-block:: bash

   paranoids.ppse.ckms:service.ykeykey_alpha.tenant.media.news.res_group.finance.portfolio

   <provider-domain> -> paranoids.ppse.ckms
   <provider-service> -> ykeykey_alpha
   <tenant-domain> -> media.news
   <resource-group> -> finance.portfolio

If you have a service called "`storage`" in a domain
"`document-manager`" that manages documents and the authorization is set
at the folder level, thus a folder is considered as the resource group,
then "`media.news`" domain having is own folder called "`media-docs`"
would represent resource as:

.. code-block:: bash

   document-manager:service.storage.tenant.media.news.res_group.media-docs

   <provider-domain> -> document-manager
   <provider-service> -> storage
   <tenant-domain> -> media.news
   <resource-group> -> media-docs

Actions
-------

Similar to :term:`resources`, actions are also controlled by the service owners.
Athenz does not own nor enforce the actions on resources. They are only
referenced in policies and used by Athenz to carry out authorization
checks and return the appropriate response to the caller.

For example, :term:`CKMS` supports a single action called "`access`" for
keygroup access but has multiple other actions ("`key-create`",
"`keyversion-activate`",
etc) defined for key management. |br|
Similarly, Sherpa might have
"`insert`", "`delete`" and "`scan`" to be defined as possible actions
for its resources. As a `tenant`, you need to follow with your provider to
see what actions are supported by the provider service.   

Policies
--------

To implement access control, we have :term:`policies` in our domain that govern
the use of our resources. For example:

.. code-block:: bash

   grant launch to aws_instance_launch_provider on service.syncer

assertion would grant the "`launch`" operation/action to all the
principals in the "`aws_instance_launch_provider`" role on the
"`service.syncer`" resource in the domain. Note that the assertion
fields are declared as general strings, as they may contain “globbing”
style wildcards, for example "`service.syncer.*`", which would apply to
any entity that matched the pattern.


Roles
-----

A :term:`role` can also delegate the determination of membership to another
trusted domain. For example, a `siteops` role managed outside a
product domain. This is how we can model tenant relations between a
provider domain and tenant domains
(see `Role Delegation <https://git.vzbuilders.com/pages/athens/athenz-guide/role_delegation/>`_).

Because roles are defined in domains, they can be partitioned by domain,
unlike users, which are global. This allows the distributed operation to
be more easily scaled.


Users Principal (NTokens)
-------------------------

All Verizon Media employees authenticated either by their *Okta/Duo*
credentials or *x.509 certificates* (based on their sshca credentials) can
be added to any role with Athenz thus granting them access to the
configured resources.

Users exist in a special domain called *user*
and are identified by their "`Okta short id`". For example, principal
`user.hga` can be added to a role granting access to some keygroup in
CKMS.

Services Principal
------------------

Principals can also be services that are authenticated by a service
management system. The term service within Athenz is more generic than a
traditional service. A service identity could represent a `command`, `job`,
`daemon`, `workflow`, as well as both a client and a service. A typical
"*headless user*" is a perfect example of a service.

For authentication,
services can use their X.509 Certificates (issued by Athenz Token
Service) or "`PrincipalTokens`" (also referred as NTokens and signed by
their private keys). The use of X.509 certificates is strongly
recommended.


Authorization Logic
-------------------

Athenz implements the following logic when determining if a given role
is allowed or denied to execute the specified action against a given
resource.

.. _fig-security-background-athenz-auth-logic:

.. figure:: /images/security/athenz/authz_logic.png
  :alt: Athenz Authorization Logic
  :width: 100%
  :align: center

  Athenz Authorization Logic


#. The application first extracts the credentials from the HTTP request which
   will be either an access token or a role x.509 certificate.
#. Then it determines what the action is the client trying to perform on what
   resource.    
#. It passes those details to the Authorization engine.
#. The engine validates the credentials and then carries out standard string
   matching against role, action and resource values against what is defined
   in Athenz :term:`policies`. 
#. The result is either an "*Allow*" or "*Deny*" based on the logic described.


.. _fig-security-background-athenz-auth-diagram:

.. figure:: /images/security/athenz/authz_diagram.png
  :alt: Athenz Authorization Diagram
  :width: 100%
  :align: center

  Athenz Authorization Diagram


Token/X.509 Certificates
------------------------

Athenz identifies users and services with both tokens and x.509
certificates. The use of x.509 certificates is strongly recommended.

Principal/Service Tokens (NToken)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A service requests its principal token from the
SIA Server which signs the token using the service’s private key. The
Principal token can then be used to access access tokens.

A principal token is serialized as a string with the following
attributes:

  version (v)
    the version of the token - S1
  domain (d)
    the domain of service.
  name (n)
    the name of the service.
  host (h)
    the FQDN of the host that issued this token
  salt (a) 
    a salt
  time (t)
    the time the token was issued
  expires (e)
    the time the token expires
  keyId (k) 
    the key identifier of the private key that was used to sign this token
  ip (i)
    the IP address where the request to get this token was initiated from
  signature (s)
    the signature of the other items

The single letter in parentheses is the key in the serialized string,
where semicolons separate key value pairs, and the key and value are
separated by an equals sign.

For example:

.. code-block:: bash

   v=U1;d=sports;n=api;h=csp1022.csp.corp.gq1.yahoo.com;a=bb4e1a837fa69a8e;t=1442255237;e=1442258837;k=0;i=10.72.42.32;s=Jw8SvYGYrk


.. note:: Requests that include a principal token should be encrypted to keep the
  token from being intercepted and reused (for the lifetime of the token).

Service Identity X.509 Certificates
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If the service is being launched within a registered provider in Athenz,
then Athenz Token Service (ZTS) can validate the service details with
the provider and, if authorized and authenticated, issue a X.509
certificate for the service identity.

The service can then use this
certificate as its identity to authenticate against other Athenz
enabled services within Verizon Media. The Service Identity principal
name (e.g. `athenz.api`) is the "*CN*" field of the X.509 Certificate Subject
issued for that service. The certificate contains a "*SAN DNS*" entry based
on the provider’s registered suffix:

.. code-block:: bash

  <service>.<domain-with-dashes>.<provider-suffix>

If the domain is a subdomain and contains `.`s, those are replaced by `-`s.
(e.g. "`athenz.prod`" domain becomes "`athenz-prod`").

For example:

.. code-block:: bash

   Subject:
       C=US,O=Oath,OU=athens.aws.us-west-2,CN=athenz.api
   X509v3 Subject Alternative Name:
       DNS:api.athenz.aws.oath.cloud

Access Tokens (ZToken)
~~~~~~~~~~~~~~~~~~~~~~

Access tokens represent an authoritative statement that a given
*principal* may assume some number of roles in a domain for a limited
period of time. Like NTokens, they are signed to prevent tampering. In a
sense, the :term:`ZTS` is an authority, like Okta, except that it is an
authority over Roles instead of Users.

An access token is a signed :abbr:`JWT (Json Web Token)`.

Role Tokens (ZToken)
~~~~~~~~~~~~~~~~~~~~

Role tokens represent an authoritative statement that a given *principal*
may assume some number of roles in a domain for a limited period of
time. Like NTokens, they are signed to prevent tampering. In a sense,
the ZTS is an authority, like Okta, except that it is an authority over
*Roles* instead of *Users*.

A role token is serialized as a string with the following attributes:

  version (v)
    the version of the token - Z1
  domain (d)
    the domain of the roles listed in the (r) field.
  roles (r)
    a list of comma-separated role names
  principal (p)
    the name of the principal (user/service) who requested this role token
  host (h)
    the FQDN of the ZTS host that issued this token
  salt (a)
    a salt
  time (t)
    the time the token was issued
  expires (e)
    the time the token expires
  keyId (k)
    the key identifier of the ZTS private key that was used to sign this token
  ip (i)
    the IP address where the request to get this token was initiated from
  signature (s)
    the signature of the other items

The single letter in parentheses is the key in the serialized string,
where semicolons separate key value pairs, and the key and value are
separated by an equals sign.

The single letter in parentheses is the key in the serialized string,
where semicolons separate key value pairs, and the key and value are
separated by an equals sign.

For example:

.. code-block:: bash

   v=Z1;d=my.domain;r=admin,editor;p=coretech.storage;h=csp1023.csp.corp.gq1.yahoo.com;a=bb4e1a837fa69a8e;t=1442255237;e=1442258837;k=0;i=10.72.42.32;s=P43Vp_LQh1

Role X.509 Certificates
~~~~~~~~~~~~~~~~~~~~~~~

Similar to Access Tokens, a role certificate represents an authoritative
statement that a given principal may assume a specific role in a domain
for a limited period of time.

Unlike Access Tokens that can be issued for several roles, a role certificate
is issued for a specific single role only.


The Role full name in the ``<domain>:role.<role>`` format is
the *CN* field of the `X.509` Certificate Subject issued for that service.
The local part in the email field in the certificate "`SAN`" field contains
the principal who obtained the role certificate.

For example if Service `athenz.api` fetch a role certificate for the
"`sports.new:role.readers`" role, the certificate will include:

.. code-block:: bash

   Subject:
       C=US,O=Oath,OU=athens.aws.us-west-2,CN=sports.news:role.readers
   X509v3 Subject Alternative Name:
       email:athenz.api@aws.athenz.cloud

Data Value Case Sensitivity
---------------------------

In Athenz, the authorization management server converts all incoming
data to lowercase before any processing - this applies to all data types
within Athenz (e.g. domain names, role names, policy names, resource
group values, etc).


Domain Registration
===================

Domains are namespaces, strictly partitioned, providing a context for
authoritative statements to be made about entities it contains. So an
administrator must first register a domain for the product he/she
manages. There are three type of domains in Athenz:

-  Top Level Product Domains
-  Subdomains
-  Personal Domains

Only system administrators can create and destroy top level domains.
Athenz top level domains are only created for registered products in
`Verizon Media Product Master <https://productmaster.vzbuilders.com/engineering/product>`_. To
create and manage subdomains the domain administrator may use
`zms-cli utility <https://git.vzbuilders.com/pages/athens/athenz-guide/zms_client/>`_ or
`Athenz UI <https://ui.athenz.ouroath.com/athenz/domain/create/domain>`_ . To
create a new subdomain domain in Athenz UI, click on ``Create`` link
present in the top right corner next to ``My Domain`` heading. Users
also have the option to
`search <https://git.vzbuilders.com/pages/athens/athenz-guide/web_ui/#search-domains>`_
for any
registered domains.

Top Level Product Domains
-------------------------

If your product does not have a top level domain already registered in
Athenz, you can file a JIRA ticket in the Jira 
`ATHENS project <https://jira.vzbuilders.com/secure/CreateIssue.jspa?pid=10388&issuetype=10100>`_.
Please provide:

-  a short and descriptive domain name for your product
-  list of administrators identified by their Okta Short IDs
-  `Verizon Media Product Id <https://productmaster.vzbuilders.com/engineering/product>`_

**Important** Verizon Media Product Id is required for all Athenz Top
Level Domains. Each registered product in Product Master can only have 1
associated Athenz Top level domain.

Subdomains
----------

.. figure:: /images/security/athenz/ui_create_subdom.png
   :alt: Create Subdomain
   :width: 100%
   :align: center

   Create Subdomain

Subdomains are created from a top level domain or another subdomain. |br|
For example, a domain called `sports` domain can have a subdomain
`sports.api` and `sports.api` can have another subdomain called
`sports.api.hockey`. To create a subdomain, the user must be a member
of the admin role of the parent domain.

It is important to note that the only relation between the parent and
sub-domain involves creation and destruction of the domains – the two
domains share no state by default, and there is no inheritance or other
relation between them other than that implied by their names.

Personal
--------

Any Verizon Media employee can create their own domain called
`home.<okta-id>`. These personal domains have the same functionality
as product domains and are primarily used for testing. Once the employee
leaves Verizon Media, the corresponding personal domain will be
automatically deleted.

.. figure:: /images/security/athenz/ui_create_personal_dom.png
   :alt: Create personal domain
   :width: 100%
   :align: center

   Create personal domain


Readings And Documentations
===========================

.. include:: /common/security/athenz/athenz-reading-resources.rst