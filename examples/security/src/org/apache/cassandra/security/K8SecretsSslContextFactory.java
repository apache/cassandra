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

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Custom {@link ISslContextFactory} implementation based on Kubernetes Secrets. It allows the keystore and
 * truststore paths to be configured from the K8 secrets via volumeMount and passwords via K8 secrets environment
 * variables.
 *
 * When keystore or truststore is updated, this implementation can detect that based on updated K8 secrets
 * environment variable ({@code KEYSTORE_UPDATED_TIMESTAMP_ENV_VAR} for the keystore and {@code
 * TRUSTSTORE_UPDATED_TIMESTAMP_ENV_VAR} for the truststore. The values in those timestamp variables is expected to
 * be numeric values. Most obvious choice might be to just use time in milliseconds but any other strategy would work
 * as well, as far as the comparison of those values can be done in a consistent/predictable manner. Again, those
 * values don't have to necessarily reflect actual file's update timestamps, using the actual file's timestamps is
 * just one of the valid options to signal updates.
 *
 * Defaults:
 * <pre>
 *     keystore path = /etc/my-ssl-store/keystore
 *     keystore password = cassandra
 *     truststore path = /etc/my-ssl-store/truststore
 *     truststore password = cassandra
 * </pre>
 *
 * Customization: In order to customize the K8 secret configuration, override appropriate values in the below cassandra
 * configuration. The similar configuration can be applied to {@code client_encryption_options}.
 * <pre>
 *     server_encryption_options:
 *       internode_encryption: none
 *       ssl_context_factory:
 *         class_name: org.apache.cassandra.security.K8SecretsSslContextFactory
 *         parameters:
 *           KEYSTORE_PATH: /etc/my-ssl-store/keystore
 *           KEYSTORE_PASSWORD_ENV_VAR: KEYSTORE_PASSWORD
 *           KEYSTORE_UPDATED_TIMESTAMP_ENV_VAR: KEYSTORE_LAST_UPDATEDTIME
 *           TRUSTSTORE_PATH: /etc/my-ssl-store/truststore
 *           TRUSTSTORE_PASSWORD_ENV_VAR: TRUSTSTORE_PASSWORD
 *           TRUSTSTORE_UPDATED_TIMESTAMP_ENV_VAR: TRUSTSTORE_LAST_UPDATEDTIME
 * </pre>
 *
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
 *       - name: KEYSTORE_PASSWORD
 *         valueFrom:
 *           secretKeyRef:
 *             name: my-ssl-store
 *             key: keystore-password
 *       - name: KEYSTORE_LAST_UPDATEDTIME
 *         valueFrom:
 *           secretKeyRef:
 *             name: my-ssl-store
 *             key: keystore-last-updatedtime
 *       - name: TRUSTSTORE_PASSWORD
 *         valueFrom:
 *           secretKeyRef:
 *             name: my-ssl-store
 *             key: truststore-password
 *       - name: TRUSTSTORE_LAST_UPDATEDTIME
 *         valueFrom:
 *           secretKeyRef:
 *             name: my-ssl-store
 *             key: truststore-last-updatedtime
 *     volumeMounts:
 *     - name: my-ssl-store
 *       mountPath: "/etc/my-ssl-store"
 *       readOnly: true
 *   volumes:
 *   - name: my-ssl-store
 *     secret:
 *       secretName: my-ssl-store
 *       items:
 *         - key: cassandra_ssl_keystore
 *           path: keystore
 *         - key: cassandra_ssl_truststore
 *           path: truststore
 * </pre>
 */
public class K8SecretsSslContextFactory extends FileBasedSslContextFactory
{
    private static final Logger logger = LoggerFactory.getLogger(K8SecretsSslContextFactory.class);

    public static final String DEFAULT_KEYSTORE_PASSWORD = "cassandra";
    public static final String DEFAULT_TRUSTSTORE_PASSWORD = "cassandra";
    public static final String DEFAULT_KEYSTORE_UPDATED_TIMESTAMP_ENV_VAR = "KEYSTORE_LAST_UPDATEDTIME";
    public static final String DEFAULT_TRUSTSTORE_UPDATED_TIMESTAMP_ENV_VAR = "TRUSTSTORE_LAST_UPDATEDTIME";

    @VisibleForTesting
    static final String DEFAULT_KEYSTORE_PASSWORD_ENV_VAR_NAME = "KEYSTORE_PASSWORD";
    @VisibleForTesting
    static final String DEFAULT_TRUSTSTORE_PASSWORD_ENV_VAR_NAME = "TRUSTSTORE_PASSWORD";

    private static final String KEYSTORE_PATH = "/etc/my-ssl-store/keystore";
    private static final String TRUSTSTORE_PATH = "/etc/my-ssl-store/truststore";
    private static final String KEYSTORE_PASSWORD_ENV_VAR = DEFAULT_KEYSTORE_PASSWORD_ENV_VAR_NAME;
    private static final String KEYSTORE_UPDATED_TIMESTAMP_ENV_VAR = DEFAULT_KEYSTORE_UPDATED_TIMESTAMP_ENV_VAR;
    private static final String TRUSTSTORE_PASSWORD_ENV_VAR = DEFAULT_TRUSTSTORE_PASSWORD_ENV_VAR_NAME;
    private static final String TRUSTSTORE_UPDATED_TIMESTAMP_ENV_VAR = DEFAULT_TRUSTSTORE_UPDATED_TIMESTAMP_ENV_VAR;

    private long keystoreLastUpdatedTime;
    private long truststoreLastUpdatedTime;

    public K8SecretsSslContextFactory()
    {
        keystore = KEYSTORE_PATH;
        keystore_password = getValueFromEnv(KEYSTORE_PASSWORD_ENV_VAR,
                                            DEFAULT_KEYSTORE_PASSWORD);
        truststore = TRUSTSTORE_PATH;
        truststore_password = getValueFromEnv(TRUSTSTORE_PASSWORD_ENV_VAR,
                                              DEFAULT_TRUSTSTORE_PASSWORD);
        keystoreLastUpdatedTime = System.currentTimeMillis();
        truststoreLastUpdatedTime = keystoreLastUpdatedTime;
    }

    public K8SecretsSslContextFactory(Map<String, Object> parameters)
    {
        super(parameters);
        keystore = getString("KEYSTORE_PATH", KEYSTORE_PATH);
        keystore_password = getValueFromEnv(getString("KEYSTORE_PASSWORD_ENV_VAR",
                                                      KEYSTORE_PASSWORD_ENV_VAR), DEFAULT_KEYSTORE_PASSWORD);
        truststore = getString("TRUSTSTORE_PATH", TRUSTSTORE_PATH);
        truststore_password = getValueFromEnv(getString("TRUSTSTORE_PASSWORD_ENV_VAR",
                                                        TRUSTSTORE_PASSWORD_ENV_VAR), DEFAULT_TRUSTSTORE_PASSWORD);
        keystoreLastUpdatedTime = System.currentTimeMillis();
        truststoreLastUpdatedTime = keystoreLastUpdatedTime;
    }

    @Override
    public synchronized void initHotReloading() {
        // No-op
    }

    /**
     * Checks environment variables for {@code K8_SECRET_KEYSTORE_UPDATED_TIMESTAMP_ENV_VAR} and {@code K8_SECRET_TRUSTSTORE_UPDATED_TIMESTAMP_ENV_VAR}
     * and compares the values for those variables with the current timestamps. In case the environment variables are
     * not valid (either they are not initialized yet, got removed or got corrupted in-flight), this method considers
     * that nothing is changed.
     * @return {@code true} if either of the timestamps (keystore or truststore) got updated;{@code false} otherwise
     */
    @Override
    public boolean shouldReload()
    {
        return hasKeystoreUpdated() || hasTruststoreUpdated();
    }

    @VisibleForTesting
    String getValueFromEnv(String envVarName, String defaultValue) {
        String valueFromEnv = StringUtils.isEmpty(envVarName) ? null : System.getenv(envVarName);
        return StringUtils.isEmpty(valueFromEnv) ? defaultValue : valueFromEnv;
    }

    private boolean hasKeystoreUpdated() {
        long keystoreUpdatedTime = getKeystoreLastUpdatedTime();
        if (keystoreUpdatedTime > keystoreLastUpdatedTime) {
            logger.info("Updating the keystoreLastUpdatedTime from oldValue {} to newValue {}",
                        keystoreLastUpdatedTime, keystoreUpdatedTime);
            keystoreLastUpdatedTime = keystoreUpdatedTime;
            return true;
        } else {
            return false;
        }
    }

    private boolean hasTruststoreUpdated() {
        long truststoreUpdatedTime = getTruststoreLastUpdatedTime();
        if (truststoreUpdatedTime > truststoreLastUpdatedTime) {
            logger.info("Updating the truststoreLastUpdatedTime from oldValue {} to newValue {}",
                        truststoreLastUpdatedTime, truststoreUpdatedTime);
            truststoreLastUpdatedTime = truststoreUpdatedTime;
            return true;
        } else {
            return false;
        }
    }

    private long getKeystoreLastUpdatedTime() {
        String keystoreUpdatedTimeEnvVarKey = getString("KEYSTORE_UPDATED_TIMESTAMP_ENV_VAR",
                                                        KEYSTORE_UPDATED_TIMESTAMP_ENV_VAR);
        String keystoreUpdatedTimeEnvVarValue = getValueFromEnv(keystoreUpdatedTimeEnvVarKey, "InvalidNumber");
        if ("InvalidNumber".equals(keystoreUpdatedTimeEnvVarValue)) {
            logger.warn("Failed to load env variable {}'s value. Will use " +
                        "existing value {}", keystoreUpdatedTimeEnvVarKey, keystoreLastUpdatedTime);
            return keystoreLastUpdatedTime;
        } else {
            return parseLastUpdatedTime(keystoreUpdatedTimeEnvVarValue, keystoreLastUpdatedTime);
        }
    }

    private long getTruststoreLastUpdatedTime() {
        String truststoreUpdatedTimeEnvVarKey = getString("TRUSTSTORE_UPDATED_TIMESTAMP_ENV_VAR",
                                                          TRUSTSTORE_UPDATED_TIMESTAMP_ENV_VAR);
        String truststoreUpdatedTimeEnvVarValue = getValueFromEnv(truststoreUpdatedTimeEnvVarKey, "InvalidNumber");
        if ("InvalidNumber".equals(truststoreUpdatedTimeEnvVarValue)) {
            logger.warn("Failed to load env variable {}'s value. Will use " +
                        "existing value {}", truststoreUpdatedTimeEnvVarKey, truststoreLastUpdatedTime);
            return truststoreLastUpdatedTime;
        } else {
            return parseLastUpdatedTime(truststoreUpdatedTimeEnvVarValue, truststoreLastUpdatedTime);
        }
    }

    private long parseLastUpdatedTime(String latestUpdatedTime, long currentUpdatedTime) {
        try
        {
            return Long.parseLong(latestUpdatedTime);
        } catch(NumberFormatException e) {
            logger.warn("Failed to parse the latestUpdatedTime {}. Will use current time {}", latestUpdatedTime,
                        currentUpdatedTime, e);
            return currentUpdatedTime;
        }
    }
}