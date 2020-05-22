..  _new_members:

New Members
===========

This page is intended to help speeding up the new hires in setting up their environment for the first time.

..  _new_members_set_up_env:

Setting up your environment
---------------------------

You have attended the `GCD orientation session <https://thestreet.ouroath.com/community/globalservicedesk/>`_ . By now, you
should be able to access the internal network on your laptop and you
should have the following:

1. email address: i.e. john.doe@verizonmedia.com
2. a user ID: i.e., djohn
3. a ``ubkey`` that you atatch to the USB port
4. Duo Mobile app installed on your phone

Sean Smith is the Champaign IT guy. He should walk you throw the following steps

**1-set up CDID Password.**

-  visit `http://yo/cdidpassword <http://yo/cdidpassword>`_
-  you’ll see a pre-populated “CDID” or Username.
-  click the button to change the password for that username.
-  Then, set a new password. Note that the username displayed is your
   own CDID (edited)

**2-set up your Yahoo Bouncer password.**

-  Sean sends an e-mail to you with a password reset link. The subject
   should something like ``*Yahoo! Bouncer: Passphrase reset*``
-  You can use that to set both your Bouncer, and your Unix (Yubikey) password.

**3-Verify ubkey is working**

Next step is to verify that your ``ubkey`` is working:

-  Open terminal
-  type the following command ``yinit``
-  you should see prompt message asking you to enter the pin. Use The
   default pin in the output message.
-  When Asked for ``PKCS-11``: enter the same default key once more.
-  If asked ``YubiKey for:``, touch and hold the ubkey until the key is
   generated.
-  Enter your Unix password (the Bouncer password) when you are asked
   for the password.


The previous process should look like the following:

.. code-block:: console

  yinit
  Please enter your PIN code for YubiKey when prompted.
  2019/01/30 10:40:33 (Default PIN code for YubiKey is XXXXXX)
  Enter PIN: <ENTER_DEFAULT_PIN=123456>
  2019/01/30 10:40:48 [INFO] Generating new touchless key in hardware......
  2019/01/30 10:40:49 [INFO] Generating new emergency key in hardware......
  Enter passphrase for PKCS#11: <ENTER_DEFAULT_PIN
  Card added: /Library/OpenSC/lib/opensc-pkcs11.so
  YubiKey for: <TOUCH_UBKEY>
  Password: <BOUNCER_PASSWORD>

A typical day to day ``ybkey`` operation would look like this:


.. code-block:: console

  $ yinit
  Enter passphrase for PKCS#11: <DEFAULT>
  Card added: /Library/OpenSC/lib/opensc-pkcs11.so
  2019/01/31 09:59:01 Refreshing your credentials...
  YubiKey for `djohn': <TOUCH_YBKEY>
  Password: <UNIX_PASSWORD_AKA_BOUNCER>
  Touch YubiKey: <TOUCH_YBKEY>
  2019/01/31 09:59:20 SSHCA credentials loaded into your ssh-agent.

**Troubleshooting**

If you have any problems initializing your ssh key:

1. First, Contact Sean
2. Reset bouncer password using the following link `http://yo/pw <http://yo/pw>`_
3. Use yo/ubkey to register your ubkey
