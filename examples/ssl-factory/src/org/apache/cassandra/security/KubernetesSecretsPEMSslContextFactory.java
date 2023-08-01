/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.security;

import java.util.Map;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Custom {@link ISslContextFactory} implementation based on Kubernetes Secrets. It allows the keystore and
 * truststore paths to be configured from the K8 secrets via volumeMount and passwords via K8 secrets environment
 * variables. The official Kubernetes Secret Spec can be found <a href="https://kubernetes.io/docs/concepts/configuration/secret/ ">here</a>.
 * <p>
 * When keystore or truststore is updated, this implementation can detect that based on updated K8 secrets
 * at the mounted paths ({@code KEYSTORE_UPDATED_TIMESTAMP_PATH} for the keystore and {@code
 * TRUSTSTORE_UPDATED_TIMESTAMP_PATH} for the truststore. The values in those paths are expected to be numeric values.
 * The most obvious choice might be to just use the time in nano/milli-seconds precision but any other strategy would work
 * as well, as far as the comparison of those values can be done in a consistent/predictable manner. Again, those
 * values do not have to necessarily reflect actual file's update timestamps, using the actual file's timestamps is
 * just one of the valid options to signal updates.
 * <p>
 * Defaults:
 * <pre>
 *     private key password = cassandra
 *     keystore updated timestamp path = /etc/my-ssl-store/keystore-last-updatedtime
 *     truststore updated timestamp path = /etc/my-ssl-store/truststore-last-updatedtime
 * </pre>
 * <p>
 * Customization: In order to customize the K8s secret configuration, override appropriate values in the below Cassandra
 * configuration. The similar configuration can be applied to {@code client_encryption_options}.
 * <pre>
 *     server_encryption_options:
 *       internode_encryption: none
 *       ssl_context_factory:
 *         class_name: org.apache.cassandra.security.KubernetesSecretsPEMSslContextFactory
 *         parameters:
 *           PRIVATE_KEY_ENV_VAR: PRIVATE_KEY
 *           PRIVATE_KEY_PASSWORD_ENV_VAR: PRIVATE_KEY_PASSWORD
 *           KEYSTORE_UPDATED_TIMESTAMP_PATH: /etc/my-ssl-store/keystore-last-updatedtime
 *           TRUSTED_CERTIFICATES_ENV_VAR: TRUSTED_CERTIFICATES
 *           TRUSTSTORE_UPDATED_TIMESTAMP_PATH: /etc/my-ssl-store/truststore-last-updatedtime
 * </pre>
 * <p>
 * Below is the corresponding sample YAML configuration for K8 env.
 * <pre>
 * apiVersion: v1
 * kind: Pod
 * metadata:
 *   name: my-pod
 *   labels:
 *     app: my-app
 * spec:
 *   containers:
 *   - name: my-app
 *     image: my-app:latest
 *     imagePullPolicy: Always
 *     env:
 *       - name: PRIVATE_KEY
 *         valueFrom:
 *           secretKeyRef:
 *             name: my-ssl-store
 *             key: private-key
 *       - name: PRIVATE_KEY_PASSWORD
 *         valueFrom:
 *           secretKeyRef:
 *             name: my-ssl-store
 *             key: private-key-password
 *       - name: TRUSTED_CERTIFICATES
 *         valueFrom:
 *           secretKeyRef:
 *             name: my-ssl-store
 *             key: trusted-certificates
 *     volumeMounts:
 *     - name: my-ssl-store
 *       mountPath: "/etc/my-ssl-store"
 *       readOnly: true
 *   volumes:
 *   - name: my-ssl-store
 *     secret:
 *       secretName: my-ssl-store
 *       items:
 *         - key: keystore-last-updatedtime
 *           path: keystore-last-updatedtime
 *         - key: truststore-last-updatedtime
 *           path: truststore-last-updatedtime
 * </pre>
 */
public class KubernetesSecretsPEMSslContextFactory extends KubernetesSecretsSslContextFactory
{
    public static final String DEFAULT_PRIVATE_KEY = "";
    public static final String DEFAULT_PRIVATE_KEY_PASSWORD = "";
    public static final String DEFAULT_TRUSTED_CERTIFICATES = "";

    @VisibleForTesting
    static final String DEFAULT_PRIVATE_KEY_ENV_VAR_NAME = "PRIVATE_KEY";
    @VisibleForTesting
    static final String DEFAULT_PRIVATE_KEY_PASSWORD_ENV_VAR_NAME = "PRIVATE_KEY_PASSWORD";
    @VisibleForTesting
    static final String DEFAULT_TRUSTED_CERTIFICATES_ENV_VAR_NAME = "TRUSTED_CERTIFICATES";

    private static final Logger logger = LoggerFactory.getLogger(KubernetesSecretsPEMSslContextFactory.class);
    private String pemEncodedKey;
    private String keyPassword;
    private String pemEncodedCertificates;
    private PEMBasedSslContextFactory pemBasedSslContextFactory;

    public boolean checkedExpiry = false;

    public KubernetesSecretsPEMSslContextFactory()
    {
        pemBasedSslContextFactory = new PEMBasedSslContextFactory();
    }

    public KubernetesSecretsPEMSslContextFactory(Map<String, Object> parameters)
    {
        super(parameters);

        pemEncodedKey = getValueFromEnv(getString(PEMConfigKey.PRIVATE_KEY_ENV_VAR, DEFAULT_PRIVATE_KEY_ENV_VAR_NAME),
                                        DEFAULT_PRIVATE_KEY);
        keyPassword = getValueFromEnv(getString(PEMConfigKey.PRIVATE_KEY_PASSWORD_ENV_VAR,
                                                DEFAULT_PRIVATE_KEY_PASSWORD_ENV_VAR_NAME),
                                      DEFAULT_PRIVATE_KEY_PASSWORD);
        pemEncodedCertificates = getValueFromEnv(getString(PEMConfigKey.TRUSTED_CERTIFICATE_ENV_VAR, DEFAULT_TRUSTED_CERTIFICATES_ENV_VAR_NAME),
                                                 DEFAULT_TRUSTED_CERTIFICATES);

        parameters.put(PEMBasedSslContextFactory.ConfigKey.ENCODED_KEY.getKeyName(), pemEncodedKey);
        parameters.put(PEMBasedSslContextFactory.ConfigKey.KEY_PASSWORD.getKeyName(), keyPassword);
        parameters.put(PEMBasedSslContextFactory.ConfigKey.ENCODED_CERTIFICATES.getKeyName(), pemEncodedCertificates);

        pemBasedSslContextFactory = new PEMBasedSslContextFactory(parameters);
    }

    @Override
    public synchronized void initHotReloading()
    {
        // No-op
    }

    @Override
    public boolean hasKeystore()
    {
        return pemBasedSslContextFactory.hasKeystore();
    }

    @Override
    protected KeyManagerFactory buildKeyManagerFactory() throws SSLException
    {
        KeyManagerFactory kmf = pemBasedSslContextFactory.buildKeyManagerFactory();
        checkedExpiry = pemBasedSslContextFactory.keystoreContext.checkedExpiry;
        return kmf;
    }

    @Override
    protected TrustManagerFactory buildTrustManagerFactory() throws SSLException
    {
        return pemBasedSslContextFactory.buildTrustManagerFactory();
    }

    public interface PEMConfigKey
    {
        String PRIVATE_KEY_ENV_VAR = "PRIVATE_KEY_ENV_VAR";
        String PRIVATE_KEY_PASSWORD_ENV_VAR = "PRIVATE_KEY_PASSWORD_ENV_VAR";
        String TRUSTED_CERTIFICATE_ENV_VAR = "TRUSTED_CERTIFICATE_ENV_VAR";
    }
}
