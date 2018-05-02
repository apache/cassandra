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

Integrated Certificate Management
---------------------------------

Motivation
^^^^^^^^^^

Cassandra can be configured to make use of SSL encryption before sending data over the network. Until recently, the only way to enable SSL was to setup a local keystore containing the required certificates. The actual steps for setting up such a keystore are not much different from the key and certificate creation process for other servers, such as Apache or other datastores. It can either be done manually using the keystore or openssl tools, or done automatically by a configuration management solution, such as puppet. In both cases, there are some challenges for securely deploying such credentials that operators need to be aware of.

*Confidentiality of private keys*. Not only must the key be protected in the local filesystem, but also securely provisioned. Generating the private key locally on a developer system and copying it to the server is not the safest option. Manually creating keys for a large cluster will be tedious and solutions such as Puppet or Ansible can help with that. But it's not trivial to setup the provisioning process in a secure way.

*Establishing a root of trust*. Each node must be able to verify that other nodes are to be trusted. This is taking place during the SSL handshake protocol by inspecting the certificate of the other node and checking if the certificate is either present in the local truststore or signed by a trusted CA. There are many practical and security considerations that can influence your decision which way to go.

*Dealing with certificate expiration*. Some people feel slightly uncomfortable when using certificates for their backend systems, as there's always the risk that certificates may expire before they have been replaced. Cassandra operators don't like the idea of getting paged late at night, because the cluster suddenly stopped working with tons of SSLHandshakeException errors in the log file. Therefor it's important to deal with certificate renewal in one way or another.

As a consequence of the pitfalls listed above, it's not uncommon to see people using certificates with long validation periods. Once deployed securely, the certificate will be used for a long time before having to go through the more or less painful renewal process again. Even if your ansible scripts are still working two years later, replacing certificates for each node in your cluster is not the most popular tasks to sign up for. So people try to avoid it, which is unfortunately not a good thing from the security perspective.

Ideally the opposite should happen. Certificates should be replaced on regular basis after a short time. This will make sure that certificates and keys will become automatically invalid shortly after they have been compromised. This is especially important since such persistent compromises are very hard to detect. An automatic certificate handling process will also be less disruptive from a operational perspective and therefor less risky to potential incidents.

Pluggable Certificate Management API
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Automatic certificate handling can be enabled by choosing one of the available implementations in the cassandra.yaml configuration. Currently only a Vault based implementation is available. But users can provide their own implementation by add a jar file with the new classes to the Cassandra lib directory and configuring the actual `CertificateIssuer <https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/vault/VaultCertificateIssuer.java>`__ implementation in the ``cassandra.yaml`` file.

.. note:: Contributions of additional implementations are most welcome!

Vault Support
^^^^^^^^^^^^^

About Vault:

    Vault secures, stores, and tightly controls access to tokens, passwords, certificates, API keys, and other secrets in modern computing. Vault handles leasing, key revocation, key rolling, and auditing. Through a unified API, users can access an encrypted Key/Value store and network encryption-as-a-service, or generate AWS IAM/STS credentials, SQL/NoSQL databases, X.509 certificates, SSH credentials, and more. https://www.vaultproject.io/

In other words, Vault is a toolbox with different security features that can be accessed through a remote API. Certificate handling in Cassandra is implemented by making use of the `PKI backend <https://www.vaultproject.io/docs/secrets/pki/index.html>`__ functionality.

See :doc:`vault` guide for more details.

