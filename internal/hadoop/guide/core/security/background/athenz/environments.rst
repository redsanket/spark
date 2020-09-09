.. _security_background_athenz_environments:

*******************
Athenz Environments
*******************

Athenz has three different environments:
`Production`, `Staging`, and `Development`.

.. sidebar:: Content is pulled from..

  * vzbuilders Github - `Athenz Guide: Athenz Environments <https://git.vzbuilders.com/pages/athens/athenz-guide/environments/index.html>`_


When validating tokens and signed policy documents, the utilities (e.g.
`ZPE policy updater`) and libraries (e.g. `ZPE`) must have access to the ZMS
and ZTS services’ public keys. Those keys are available through `Ykeykey`. |br|
The keygroup is identified as public in `Ykeykey` so that no on-boarding
and/or authorization is necessary to access those keys. |br|
The only requirement is to make sure that package is installed and configured
correctly to point to the appropriate `Ykeykey` environment (e.g. `prod`,
`corp`).

In addition to `ykeykey`, the ZMS and ZTS public keys are also available through a
configuration file that is shipped with a dist package.

* `athenz_config <https://dist.corp.yahoo.com/by-package/athenz_config>`_
  - Production Environment
* `athenz_config_stg <https://dist.corp.yahoo.com/by-package/athenz_config_stg>`_
  - Staging Environment
* `athenz_config_dev <https://dist.corp.yahoo.com/by-package/athenz_config_dev>`_
  - Development Environment
   
Production
==========

-  **UI:**
   `ui.athenz.ouroath.com <https://ui.athenz.ouroath.com/athenz>`_
-  **ZMS - AWS:** ``zms.athenz.ouroath.com:4443`` (HTTPS)
-  **ZTS - AWS:** ``zts.athenz.ouroath.com:4443`` (HTTPS)
-  **ZTS - On-Prem:** ``zts.athens.yahoo.com:4443`` (HTTPS)


The `athens_public_keys <https://dist.corp.yahoo.com/by-package/athens_public_keys>`_
package must be installed. By default it points to `Ykeykey` prod
environment. The package provides a `yinst` setting called "`svc_type`" which
can be used to configure `ykeykey` to look for those keys in corp
environment if your hosts are in corp zone instead of prod.

During manual installation of the package, the post-activate script
automatically determines what environment access is available and
configures the `svc_type` setting as such. However, if the package is
deployed through automated deployment through its state file, make sure
the state file has the appropriate setting. |br|
For example:

  .. code-block:: bash

     add yinst setting athens_public_keys.svc_type=corp

Integration/Staging
===================

-  **UI:**
   `stage-ui.athenz.ouroath.com <https://stage-ui.athenz.ouroath.com/athenz>`_
-  **ZMS - AWS:** ``stage.zms.athenz.ouroath.com:4443`` (HTTPS)
-  **ZTS - AWS:** ``stage.zts.athenz.ouroath.com:4443`` (HTTPS)
-  **ZTS - On-Prem:** ``stg.zts.athens.yahoo.com:4443`` (HTTPS)

Integration/Staging environments should only be using during integration
phase where the certificates and tokens retrieved from this environment
are not used to make calls to external services. Same requirement
applies to deployment pipeline functional tests.

If you’re expecting to use the certificates and tokens received from this
environment to be used against other services then the production environment
must be used for your deployment pipeline.

Public Key Access for staging is the same as `Development`.

.. _security_background_athenz_environments_dev:

Development
===========

-  **UI:**
   `dev-ui.athenz.vzbuilders.com <https://dev-ui.athenz.vzbuilders.com>`_
-  **ZMS - On-Prem:** ``dev.zms.athens.yahoo.com:4443`` (HTTPS)
-  **ZTS - On-Prem:** ``dev.zts.athens.yahoo.com:4443`` (HTTPS)

.. note::

   The following aspects are different between AWS and On-Prem ZTS
   instance:
   
   ZTS - AWS:
     only accepting X.509 client certificates, supporting AWS providers, and
     issuing AWS temporary credentials.
   ZTS - On-Prem:
     supporting NToken in addition to X.509 client certificates, supporting
     OpenStack provider, and no support for AWS temporary credentials.

The `athens_public_keys_test <https://dist.corp.yahoo.com/by-package/athens_public_keys_test>`_
package must be installed. By default it points to `Ykeykey` alpha
environment. Those keys are only available in the `Ykeykey` alpha
environment so no other configuration settings are necessary.

