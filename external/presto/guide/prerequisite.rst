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

Steps:
  1. Go to your encryption zone in `Support Shop <https://supportshop.cloud.corp.yahoo.com:4443/doppler/ez>`_.
  2. If the ``Encryption Key Readers`` section does not already list ``Presto`` as a service, click on ``Edit Key Readers`` and select ``Presto`` from the drop down.
  3. Click ``Add Service`` and then click ``Update EZ Key Readers``.

  .. image:: images/ez_add_presto_service.png
     :height: 516px
     :width: 883px
     :scale: 80%
     :alt:
     :align: left

  4. After submission, wait for the admin of your encryption zone to approve the request.
  5. Once approved, Support Shop will create a Jira for the Grid SE to modify to the ACL.

