Prerequisites
#############

.. _prerequisities:

.. _acl:

ACL
***
If you are going to run Presto queries from Gateway or Hue, you can skip this step.

If you are going to run from a Launcher or setting up a Looker
or Tableau server, ACLs need to be open from that host to port 4443 of the
Presto cluster nodes.

You can check if the ports are open by running
``nc -z prestocoordinator_hostname 4443`` on the client host.

For example:

.. code-block:: text

    -bash-4.1$ nc -zv xandarblue-presto.blue.ygrid.yahoo.com 4443
    Connection to xandarblue-presto.blue.ygrid.yahoo.com 4443 port [tcp/pharos] succeeded!

    -bash-4.1$ nc -zv xandartan-presto.tan.ygrid.yahoo.com 4443
    nc: connect to xandartan-presto.tan.ygrid.yahoo.com port 4443 (tcp) failed: Connection timed out

YGRID
=====

Search `Policy Manager <http://yo/pes>`_

  .. image:: images/pes_advanced_search.png
     :height: 516px
     :width: 883px
     :scale: 80%
     :alt:
     :align: left

If ACL is not open, refer to `Policy Manager documentation <https://git.ouroath.com/pages/pes/pes-docs/>`_
on how to request access. In most cases, you will have to get Paranoid approval
before requesting for ACL. Updates to ACL are pushed only twice a week on Tuesday
and Thursday. Plan accordingly.

The PES Resource `Grid Web Services GRID::WS_PROD TCP::1024_65535::4080:4443 <https://pes-ui.corp.yahoo.com/pes/domain/hadoop/resource/pes.acl%3Agrid.ws_prod.4065b556-ee2c-3728-84f7-b7d5458edb89/workloads>`_
can be used to request access to ygrid Presto clusters in all the colos.


VCG
===
By Paranoid mandate, any tool accessing VCG should be hosted in VCG backplane
itself and so does not require opening ACLs through Policy Manager.
For connectivity between Verizon and VCG, we are in the process of setting up proxies.
*... To be updated ...*.


Encryption zones
****************
If your data is in an encryption zone, you need to grant Presto access to the data.
If you are a consumer of the data, you need to request access from the Encryption Zone admins.

Steps:
  1. Go to the encryption zone page in `Support Shop <https://supportshop.cloud.corp.yahoo.com:4443/doppler/ez>`_ and find the encryption zone of your interest.
  2. If the ``Encryption Key Readers`` section does not already list ``Presto`` as a service, click on ``Edit Key Readers`` and select ``Presto`` from the drop down.
  3. Click ``Add Service`` and then click ``Update EZ Key Readers``.

  .. image:: images/ez_add_presto_service.png
     :height: 516px
     :width: 883px
     :scale: 80%
     :alt:
     :align: left

  4. Both ``Presto`` service and the user that will be running the queries need access to the encryption zone. If the user (regular or headless) or the unix group they belong to is not part of the ``Encryption Key Readers`` section, request access for that as well via ``Add User`` or ``Add Group``.
  5. After submission, wait for the admin of the encryption zone to approve the request.
  6. Once approved, Support Shop will create a Jira for the Grid SE to modify the ACL. Please wait for the ACL push to happen and Jira to be resolved. It may take 3-5 business days based on the deploy cycle.

