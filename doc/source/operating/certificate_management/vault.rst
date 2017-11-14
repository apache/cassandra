.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at
..
..     http://www.apache.org/licenses/LICENSE-2.0
..
.. Unless required by applicable law or agreed to in writing, software
.. distributed under the License is distributed on an "AS IS" BASIS,
.. WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
.. See the License for the specific language governing permissions and
.. limitations under the License.

.. highlight:: none

Vault PKI Support
-----------------

Cassandra can be configured to enable automatic SSL certificate management using Vault. Certificates will be requested by Cassandra and issued by Vault upon successful authentication. Please note that Vault version 0.9.0 or later is required for this feature.

Connection Settings
^^^^^^^^^^^^^^^^^^^

The Vault server address must be configured in ``cassandra.yaml`` before using any of the Vault based features.

::

    # Address of Vault instance that should be used for certificate and key management.
    vault_address: https://vault.example:8200
    # File at least a single PEM formatted certificate that is used for ssl endpoint verification. This should either be
    # the Vault certificate (as configured as "tls_cert_file") or a signing CA certificate.
    vault_cert_file: /etc/cassandra/vault-cert.pem

Although optional, it's highly recommended to provide a certificate as ``vault_cert_file`` that can be used to verify the Vault HTTPS connection. Any men-in-the-middle attack that manages to read or manipulate the communication between your Cassandra nodes and Vault will ultimately break your system security.

Preparing Vault
^^^^^^^^^^^^^^^

After `installing <https://www.vaultproject.io/docs/install/index.html>`__ and `configuring <https://www.vaultproject.io/docs/configuration/index.html>`__ Vault for general use, you'll also have to setup a Vault `PKI Backend <https://www.vaultproject.io/docs/secrets/pki/index.html>`__ using the following steps:

::

    # Mount PKI backend under custom path. It's recommended to mount individual backends for your clusters.
    # https://www.vaultproject.io/intro/getting-started/secret-backends.html
    vault secrets enable -path=/pki/cas-cluster pki

    # Set max ttl that is used as expiration date for your root certificate
    vault secrets tune -max-lease-ttl=87600h pki/cas-cluster

    # Generate root CA
    # https://www.vaultproject.io/api/secret/pki/index.html#generate-root
    vault write /pki/cas-cluster/root/generate/internal common_name=myvault.com ttl=87600h

    # Configure URLs (change to public accessible address of your Vault instance)
    vault write /pki/cas-cluster/config/urls issuing_certificates="http://127.0.0.1:8200/v1/pki/cas-cluster/ca" crl_distribution_points="http://127.0.0.1:8200/v1/pki/cas-cluster/crl"

    # Set duration for which the generated CRL should be marked valid (optional)
    # You may set this to a short, non-default value in case you plan to actively monitor the audit log
    # to be able to promptly revoke certificates.
    #vault write /pki/cas-cluster/config/crl expiry="1h"

    # Create policy for certificate creation (will be referenced in next step)
    # https://www.vaultproject.io/docs/concepts/policies.html
    cat <<EOT | vault policy write pki/cas-cluster/member/cluster_node -
    path "/pki/cas-cluster/issue/*" {
       policy = "write"
    }
    EOT

Please note that there may be other ways to setup Vault. E.g. depending on your situation, it may make sense to import the root certificate from an external source or to use intermediate CAs. Although different approaches may work for expert users, only the approach described in this document has been specifically tested for Cassandra.

Authentication
^^^^^^^^^^^^^^

Certificates used by Cassandra need to be trusted and validated by other nodes and clients. It's therefor essential that Vault is configured to only issue certificates to nodes that can be successfully authenticated. The issued certificate must prove that the owner is a Cassandra process 1) running on a officially designated system and 2) is executing the genuine Cassandra code. We do not want anyone to be able to copy and use the certificate and also want to prevent e.g. a simple malicious script on the Cassandra node to interfere with the cluster.

AppRole Authentication
~~~~~~~~~~~~~~~~~~~~~~

Vault comes with a number of supported `auth backends <https://www.vaultproject.io/api/auth/index.html>`__. Currently only the AppRole backend is supported by Cassandra.

Using the AppRole backend tries to addresses the two requirements mentioned above in different ways. First of all, systems allowed to request new certificates will be whitelisted based on the IP address or address ranges. As additional measure, a secret token value exists that is assumed to be only known to the running Cassandra process and stored safely on the node.

.. note:: Contributions for support of additional auth backends are most welcome!

In the following steps, we're going to create an individual AppRole ``cas-node1.vlan1`` for a single node. The node will only be allowed to authenticate from address ``10.1.2.3`` using it's own secret Id. Alternatively you may also think about creating an AppRole for racks or DCs, where each node would have to share the same secret ID. Eventually all AppRole roles will get a grant for the ``cluster_node`` PKI role, which defines allowed parameters for generating new certificates.

::

    # Create AppRole, see following URL for parameters.
    # https://www.vaultproject.io/api/auth/approle/index.html#create-new-approle
    vault auth enable approle
    vault write auth/approle/role/cas-node1.vlan1 bind_secret_id=true bound_cidr_list='10.1.2.3/32' token_num_uses=1 token_ttl=60 token_max_ttl=120 policies=pki/cas-cluster/member/cluster_node

    # Read role_id that has to be used for login (will be needed later)
    vault read auth/approle/role/cas-node1.vlan1/role-id

    # Generate secret_id (will be needed later)
    # This is not required if bind_secret_id=false has been used for AppRole creation in the step above
    vault write -f auth/approle/role/cas-node1.vlan1/secret-id

    # Create PKI role. Please change parameters as needed, see following URL.
    # https://www.vaultproject.io/api/secret/pki/index.html#create-update-role
    vault write pki/cas-cluster/roles/cluster_node \
     allowed_domains="example.com" \
     allow_localhost="true" \
     allow_bare_domains="true" \
     allow_subdomains="true" max_ttl="72h"

The authenticator can now be configured in the ``cassandra.yaml`` file as follows:

::

    vault_authenticator:
        - class_name: org.apache.cassandra.vault.AppRoleAuthenticator
          parameters:
            - id_file_path: /etc/cassandra/vault_approle.properties

The specified properties file needs to contain the role ID (see above) and an (optional) secret value.

::

    role_id: <role ID UUID>
    secret_id: <secret ID UUID>

Eventually an ``Authenticating using AppRole`` message should appear in the ``system.log`` if configured correctly and a VaultIssuer has been enabled as well (see below). Upon successful authentication, a access token is provided that can be used to authorize certificate requests.

VaultCertificateIssuer
^^^^^^^^^^^^^^^^^^^^^^

Certificates can either by read from a local keystore or acquired by `CertificateIssuer <https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/security/CertificateIssuer.java>`__ implementations. An overview of the approach and motivations can be found in the :doc:`index` description.

The Vault implementation can be used for both clients (native transport) and inter-node messaging. It can be configured individually as follows.

::

    server_certificate_issuer:
      - class_name: org.apache.cassandra.vault.VaultCertificateIssuer
        parameters:
            # URI prefix where the Vault PKI backend has been mounted
          - pki_path: /v1/pki/cas-cluster
            # Role mapped to policy used to generate credentials
            role: cluster_node
            # How many days before certificate expiration should we start trying to acquire a new certificate?
            # Omitting the value or setting it to < 0 will disable certificate renewal
            renew_days_before_expire: 5
            # Indicates if credentials should be stored in the local keystore configured above
            use_keystore: true
            # Common name the certificate should be issued for (required)
            common_name: cassandra1.vlan1
            # It's recommended to add an IP naming extension, as the hostname will not always be available
            ip_sans: 10.1.2.3
            # Optional alternative names
            alt_names: cassandra1.local

    client_certificate_issuer:
      - class_name: org.apache.cassandra.vault.VaultCertificateIssuer
        parameters:
          - pki_path: /v1/pki/cas-cluster
            role: cluster_node
            renew_days_before_expire: 5
            use_keystore: true
            common_name: cassandra1.vlan1
            ip_sans: 10.1.2.3
            alt_names: cassandra1.local

Relation to Other Encryption Settings
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You must enable encryption for either server, client or both kind of connections to use Vault.

Acquired certificates can be store in a local keystore, if enabled by the ``use_keystore`` option. In that case, the keystore and keystore specific settings from ``server_encryption_options``/``client_encryption_options`` will be used and a new keystore will be created if it does not already exists under the specified path. Keeping a local keystore will allow your nodes to start and join the cluster using an existing certificates even when Vault is not available.

It's also highly recommended to enable both ``require_client_auth`` and ``require_endpoint_verification``. Nodes can always be mutually authenticated and certificates will always contain the node's address information, if Vault has been configured correctly.

Truststore Settings
^^^^^^^^^^^^^^^^^^^

Certificates used by Cassandra are either signed through a CA (certificate authority) or self-signed. In either case, nodes need to be able to validate such certificates by using a truststore that contains either the CA certificate or all self-signed node specific certificates.

.. important:: The Vault certificate issuer will *not* be responsible for the truststore. You have to manage it by yourself. The good news is, that CA certificates tend to be long lived and do not come with a private key. It's perfectly fine to have a single truststore or CA certificate file and provision it for all of your nodes.

In case of Vault, the CA certificate can be retrieved through the `Read CA Certificate <https://www.vaultproject.io/api/secret/pki/index.html#read-ca-certificate>`__ call at ``/v1/<pki-mount-point>/ca/pem``. You need to import it either afterwards into the truststore or simply store the PEM formatted CA certificate as a file and use the ``ca_file`` setting in ``server_encryption_options``.

Certificate Revocation
^^^^^^^^^^^^^^^^^^^^^^

.. note:: Revocation may be obsolete in case of short-live certificates. `Read more <https://www.imperialviolet.org/2014/04/19/revchecking.html>`__.

Certificates issued by Vault can be `revoked <https://www.vaultproject.io/api/secret/pki/index.html#revoke-certificate>`__ if needed. The current list of revocations will be provided as CSR distribution endpoint accessible using the URL configured in the `Preparing Vault`_ step.

Please note that the JVM will not check a certificate's revocation status unless it has been configured to do so. Search for "checkRevocation" and "enableCRLDP" in the `JSSE reference <https://docs.oracle.com/javase/8/docs/technotes/guides/security/jsse/JSSERefGuide.html>`__ for details. You may also be interested in `Vault OCSP <https://github.com/T-Systems-MMS/vault-ocsp>`__ support and `corresponding security properties <https://docs.oracle.com/javase/8/docs/technotes/guides/security/certpath/CertPathProgGuide.html#AppC>`__.
