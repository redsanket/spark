Python Client
#############

Setup
*****
Presto has a `python client <https://github.com/prestodb/presto-python-client>`_
which can be installed through `pip <https://pypi.org/project/presto-python-client/>`_.

.. code-block:: text

  pip install presto-python-client

For kerberos authentication, you need krb5 1.10.3-60.el6 and above installed as we
make use of the ``dns_canonicalize_hostname`` setting added in that version. If
you are still on RHEL 6, verify if your node has the required version of packages.
For example:

.. code-block:: text

  -bash-4.1$ rpm -qa | grep krb
  krb5-devel-1.10.3-65.el6.x86_64
  krb5-workstation-1.10.3-65.el6.x86_64
  krb5-libs-1.10.3-65.el6.x86_64
  krb5-devel-1.10.3-65.el6.i686
  krb5-libs-1.10.3-65.el6.i686
  compat-krb5bin-1.0.0-1.noarch
  pam_krb5-2.3.11-9.el6.x86_64

Coding
======
The Presto server is behind a VIP (Load balancer) and so we need the server
principal name to have same name as the host specified instead of canonicalized
host name. Python's requests_kerberos module does not have an option to do it
through code and so we have to set `dns_canonicalize_hostname`` to ``false`` in
krb5.conf. Below is a code example that does that and connects to Xandar Blue
Presto server and does a simple query.


.. code-block:: python

  import prestodb
  from prestodb.auth import KerberosAuthentication
  from requests_kerberos import OPTIONAL
  import os

  KRB5_CONFIG = "krb5.conf"
  PRESTO_HOST = 'xandarblue-presto.blue.ygrid.yahoo.com'
  os.environ['KRB5_CLIENT_KTNAME'] = '/home/p_search/p_search.prod.headless.keytab'

  #Generates new krb5.conf from /etc/krb5.conf with dns_canonicalize_hostname setting as false
  def generate_krb_conf():
      current_krb5_conf = '/etc/krb5.conf'
      dns_canonicalize_hostname = ' dns_canonicalize_hostname = false\n'
      with open(current_krb5_conf, 'r') as fp:
          content = fp.readlines()
          for line in range(len(content)):
              if 'libdefaults' in content[line]:
                  content.insert(line+1, dns_canonicalize_hostname)
                  break

      with open(KRB5_CONFIG, 'w') as fp:
          for line in content:
              fp.write("%s" % line)

  generate_krb_conf()
  _auth = KerberosAuthentication(
      config=KRB5_CONFIG,
      service_name='HTTP',
      mutual_authentication=OPTIONAL,
      hostname_override=PRESTO_HOST,
      principal='p_search@YGRID.YAHOO.COM'
  )

  conn = prestodb.dbapi.connect(
      host=PRESTO_HOST,
      port=4443,
      catalog='dilithiumblue',
      schema='benzene',
      http_scheme='https',
      auth=_auth,
  )

  cur = conn.cursor()
  cur.execute('SHOW tables')
  rows = cur.fetchall()

  print(rows)
