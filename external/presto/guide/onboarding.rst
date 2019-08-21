Onboarding
##########

::

        * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
        * DISCLAIMER: We are currently not onboarding new users/projects onto Presto except in VCG. *
        * Onboarding will be available starting sometimes in Q1 of 2020.                            *
        * This information is for planning purposes only.                                           * 
        * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
        
Deployment Model
****************

We run Presto in a separate cluster that can access data in any of the regular compute clusters within the same colo.

Our plan is to have two multitenant clusters in each colo: one for Staging, and one for Production.

The **Staging** cluster is intended for use case evaluation, testing, and resource estimation needed for production onboarding.

The **Production** cluster will have a dedicated queue for each onboarded project.

Current State
*************

Information is here:  :doc:`Deployment on Git <https://git.ouroath.com/pages/hadoop/docs/presto/deployment.html>`

Onboarding Process
******************

Onboarding will be done via `Doppler <https://yo/doppler>`_. The process consists of answering several questions regarding the use case as well as running experiments on a staging cluster. The goal of this process is to determine the resources needed by the project. 

Once the ask is clarified, the user needs to go through the standard CAR and procurement process. Once the hardware is received and configured, a dedicated queue in the production cluster will be created.

.. code-block:: text

  [THIS SECTION will need to be completed later once we have an onboarding process in place.]
