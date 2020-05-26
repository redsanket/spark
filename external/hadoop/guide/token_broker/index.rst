.. _token_broker:

============
Token-broker
============


.. _on_borading:

Onboarding
==========

**Athenz**

- `copper argos <https://yahoo.github.io/athenz/site/copper_argos/>`_
- `copper argos dev <https://yahoo.github.io/athenz/site/copper_argos_dev/>`_


**Grid Token Broker**

- `Grid Athenz Token Broker Design <https://docs.google.com/document/d/1TIWXfqG1JKpd3ITF42JRSdNnS6Dx828WzRY6RbGcwA0/edit?usp=sharing>`_
- `Token Broken Design Notes <https://docs.google.com/document/d/1bRvKmxEUotpW2A8BcBV1q-DpN0rSFnex2c5MUjvTeOA/edit?usp=sharing>`_
- `Grid Token Broken Provider Architecture Proposal <https://drive.google.com/open?id=12I0FNB6bPVqHi8ibjMEQ5QsPe4SZQYVF46m6ZvAeaAo>`_
- `code repository <https://git.ouroath.com/hadoop/token-broker>`_


**Examples**

Vespa is really the best example as they have many of the problems. For example they have forked
the athenz client to: 1) remove dependencies that will conflict with users and 2) disable athenz
caching feature for multiple users cause issues.
- `vespa repository <https://github.com/vespa-engine/vespa.git>`_
- `instance provider <https://github.com/yahoo/athenz/tree/master/examples/java/instance-provider>`_
- `gcp token-broker <https://github.com/GoogleCloudPlatform/gcp-token-broker>`_


**Action Items**

- `Grid Token Broker Action Plan <https://docs.google.com/spreadsheets/d/1GMNurZWgXle4LC-B7o2RULUAire4U-lyKd1PutXofMY/edit#gid=0>`_

**Recorded meetings**

- `YCA EOL <https://drive.google.com/open?id=1_pyeU5iLSDPojUW25hGOHXLHnkDeBSPA>`_
- `Grid Infrastructure Copper Argos Proposal <https://drive.google.com/open?id=1K09Ti-QCVOs8hFN5dzeiIfVpAMmMh8ll>`_
