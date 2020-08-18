.. topic:: Definitions
   :class: definitionbox

   .. glossary::

       Authentication
         `Confirming your own identity`. Authentication is the act of validating that
         users are who they claim to be. `Passwords` are the most common authentication
         factor—if a user enters the correct password, the system assumes the
         identity is valid and grants access. |br|
         In some instances, systems require the successful verification of more than
         one factor before granting access.
         This :abbr:`MFA (multi-factor authentication)` requirement is often deployed
         to increase security beyond what passwords alone can provide.

       Authorization
         Is the process of giving the user permission to access a specific resource
         or function. This term is often used interchangeably with access control
         or client privilege. In secure environments, authorization must always
         follow authentication—users should first prove that their identities are
         genuine before an organization’s administrators grant them access to the
         requested resources.
       
       CKMS
         Centralized Key Management System (CKMS).
       
       domains
         Domains are namespaces, strictly partitioned, providing a context for
         authoritative statements to be made about entities it contains. Referring
         to the data model diagram in :numref:`fig-security-background-athenz-data-model`,
         entities stored in a domain are roles, policies and services.
         (See :numref:`security_background_athenz_concepts_domains`). 
       
       subdomains
         domains that start with the parent domain as a prefix, using a "`.`" as a
         domain delimiter. For example, "`media`" is a top level domain,
         and "`media.news`" is a subdomain of it.
         (See :numref:`security_background_athenz_concepts_domains`). 
       
       TLS
         Transport Layer Security is are cryptographic protocols designed to provide
         communications security over a computer network.
         See (`TLS Wikipedia <https://en.wikipedia.org/wiki/Transport_Layer_Security>`_).
       
       mutual TLS
         Mutual TLS is a common security practice that uses client TLS
         certificates to provide an additional layer of protection, allowing to
         cryptographically verify the client information. |br|
         The TLS standard allows specifying the client certificate as well, so the
         server can accept connections only for clients with certificates registered
         with the server certificate authority, or provide additional security
         checks based on the information stored in the client certificate.
         This is what we call “Mutual TLS” - when both sides of the connection
         verify certificates.
       
       policies
         Is a set of assertions ("`rules`") about granting or denying an
         operation/action on a resource.
       
       Principal
         The actors in Athenz that can assume a role are called principals.
         There are two types of principals in Athenz: `users` and `services`.  
       
       RBAC
         Role-Based Authorization Control is a fine grained role-based access control
         support for a centralized management system with support for control-plane
         access control decisions and a decentralized enforcement mechanism
         suitable for data-plane access control decisions.
       
       resources
         Resources in Athenz aren't explicitly modeled. They are referred to by
         name. A resource is something that is "`owned`" and controlled in a
         specific domain by the provider service. For example, for :term:`CKMS` a
         resource is a keygroup. For Sherpa, a resource is table. For AWS temporary
         credentials support, a resource is an IAM role name. So each service using
         Athenz for authorization is responsible for defining their own resources
         and referencing them in their policy rules (`assertions`).
       
       role
         In Athenz, a role can be thought of as a group, anyone in the group can 
         *assume* the role that takes a particular action. Every policy assertion
         describes what can be done by a role.
       
       CSR
         Certificate Signing Request
       
       SIA
         Service Idenditiy Agent
       
       ZMS
         AuthoriZation Management System  
       
       ZTS
         authoriZation Token System. (Athenz Token Service)