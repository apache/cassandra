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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.EncryptionOptions;

/**
 * Custom {@link ISslContextFactory} implementation based on Kubernetes Secrets. It allows the keystore and
 * truststore paths to be configured from the K8 secrets via volumeMount and passwords via K8 secrets environment
 * variables. The official Kubernetes Secret Spec can be found <a href="https://kubernetes.io/docs/concepts/configuration/secret/ ">here</a>.
 *
 * When keystore or truststore is updated, this implementation can detect that based on updated K8 secrets
 * at the mounted paths ({@code KEYSTORE_UPDATED_TIMESTAMP_PATH} for the keystore and {@code
 * TRUSTSTORE_UPDATED_TIMESTAMP_PATH} for the truststore. The values in those paths are expected to be numeric values.
 * The most obvious choice might be to just use the time in nano/milli-seconds precision but any other strategy would work
 * as well, as far as the comparison of those values can be done in a consistent/predictable manner. Again, those
 * values do not have to necessarily reflect actual file's update timestamps, using the actual file's timestamps is
 * just one of the valid options to signal updates.
 *
 * Defaults:
 * <pre>
 *     keystore path = /etc/my-ssl-store/keystore
 *     keystore password = cassandra
 *     keystore updated timestamp path = /etc/my-ssl-store/keystore-last-updatedtime
 *     truststore path = /etc/my-ssl-store/truststore
 *     truststore password = cassandra
 *     truststore updated timestamp path = /etc/my-ssl-store/truststore-last-updatedtime
 * </pre>
 *
 * Customization: In order to customize the K8s secret configuration, override appropriate values in the below Cassandra
 * configuration. The similar configuration can be applied to {@code client_encryption_options}.
 * <pre>
 *     server_encryption_options:
 *       internode_encryption: none
 *       ssl_context_factory:
 *         class_name: org.apache.cassandra.security.KubernetesSecretsSslContextFactory
 *         parameters:
 *           KEYSTORE_PASSWORD_ENV_VAR: KEYSTORE_PASSWORD
 *           KEYSTORE_UPDATED_TIMESTAMP_PATH: /etc/my-ssl-store/keystore-last-updatedtime
 *           TRUSTSTORE_PASSWORD_ENV_VAR: TRUSTSTORE_PASSWORD
 *           TRUSTSTORE_UPDATED_TIMESTAMP_PATH: /etc/my-ssl-store/truststore-last-updatedtime
 *       keystore: /etc/my-ssl-store/keystore
 *       truststore: /etc/my-ssl-store/truststore
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
 *       - name: TRUSTSTORE_PASSWORD
 *         valueFrom:
 *           secretKeyRef:
 *             name: my-ssl-store
 *             key: truststore-password
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
 *         - key: keystore-last-updatedtime
 *           path: keystore-last-updatedtime
 *         - key: cassandra_ssl_truststore
 *           path: truststore
 *         - key: truststore-last-updatedtime
 *           path: truststore-last-updatedtime
 * </pre>
 */
public class KubernetesSecretsSslContextFactory extends FileBasedSslContextFactory
{
    private static final Logger logger = LoggerFactory.getLogger(KubernetesSecretsSslContextFactory.class);

    /**
     * Use below config-keys to configure this factory.
     */
    public interface ConfigKeys {
        String KEYSTORE_PASSWORD_ENV_VAR = "KEYSTORE_PASSWORD_ENV_VAR";
        String TRUSTSTORE_PASSWORD_ENV_VAR = "TRUSTSTORE_PASSWORD_ENV_VAR";
        String KEYSTORE_UPDATED_TIMESTAMP_PATH = "KEYSTORE_UPDATED_TIMESTAMP_PATH";
        String TRUSTSTORE_UPDATED_TIMESTAMP_PATH = "TRUSTSTORE_UPDATED_TIMESTAMP_PATH";
    }

    public static final String DEFAULT_KEYSTORE_PASSWORD = "";
    public static final String DEFAULT_TRUSTSTORE_PASSWORD = "";

    @VisibleForTesting
    static final String DEFAULT_KEYSTORE_PASSWORD_ENV_VAR_NAME = "KEYSTORE_PASSWORD";
    @VisibleForTesting
    static final String DEFAULT_TRUSTSTORE_PASSWORD_ENV_VAR_NAME = "TRUSTSTORE_PASSWORD";

    private static final String KEYSTORE_PATH_VALUE = "/etc/my-ssl-store/keystore";
    private static final String TRUSTSTORE_PATH_VALUE = "/etc/my-ssl-store/truststore";
    private static final String KEYSTORE_PASSWORD_ENV_VAR_NAME = DEFAULT_KEYSTORE_PASSWORD_ENV_VAR_NAME;
    private static final String KEYSTORE_UPDATED_TIMESTAMP_PATH_VALUE = "/etc/my-ssl-store/keystore-last-updatedtime";
    private static final String TRUSTSTORE_PASSWORD_ENV_VAR_NAME = DEFAULT_TRUSTSTORE_PASSWORD_ENV_VAR_NAME;
    private static final String TRUSTSTORE_UPDATED_TIMESTAMP_PATH_VALUE = "/etc/my-ssl-store/truststore-last-updatedtime";

    private final String keystoreUpdatedTimeSecretKeyPath;
    private final String truststoreUpdatedTimeSecretKeyPath;
    private long keystoreLastUpdatedTime;
    private long truststoreLastUpdatedTime;

    public KubernetesSecretsSslContextFactory()
    {
        keystoreContext = new FileBasedStoreContext(getString(EncryptionOptions.ConfigKey.KEYSTORE.toString(), KEYSTORE_PATH_VALUE),
                                                    getValueFromEnv(KEYSTORE_PASSWORD_ENV_VAR_NAME, DEFAULT_KEYSTORE_PASSWORD));

        trustStoreContext = new FileBasedStoreContext(getString(EncryptionOptions.ConfigKey.TRUSTSTORE.toString(), TRUSTSTORE_PATH_VALUE),
                                                      getValueFromEnv(TRUSTSTORE_PASSWORD_ENV_VAR_NAME, DEFAULT_TRUSTSTORE_PASSWORD));

        keystoreLastUpdatedTime = System.nanoTime();
        keystoreUpdatedTimeSecretKeyPath = getString(ConfigKeys.KEYSTORE_UPDATED_TIMESTAMP_PATH,
                                                     KEYSTORE_UPDATED_TIMESTAMP_PATH_VALUE);
        truststoreLastUpdatedTime = keystoreLastUpdatedTime;
        truststoreUpdatedTimeSecretKeyPath = getString(ConfigKeys.TRUSTSTORE_UPDATED_TIMESTAMP_PATH,
                                                       TRUSTSTORE_UPDATED_TIMESTAMP_PATH_VALUE);
    }

    public KubernetesSecretsSslContextFactory(Map<String, Object> parameters)
    {
        super(parameters);
        keystoreContext = new FileBasedStoreContext(getString(EncryptionOptions.ConfigKey.KEYSTORE.toString(), KEYSTORE_PATH_VALUE),
                                                    getValueFromEnv(getString(ConfigKeys.KEYSTORE_PASSWORD_ENV_VAR,
                                                                              KEYSTORE_PASSWORD_ENV_VAR_NAME), DEFAULT_KEYSTORE_PASSWORD));

        trustStoreContext = new FileBasedStoreContext(getString(EncryptionOptions.ConfigKey.TRUSTSTORE.toString(), TRUSTSTORE_PATH_VALUE),
                                                      getValueFromEnv(getString(ConfigKeys.TRUSTSTORE_PASSWORD_ENV_VAR,
                                                                                TRUSTSTORE_PASSWORD_ENV_VAR_NAME), DEFAULT_TRUSTSTORE_PASSWORD));
        keystoreLastUpdatedTime = System.nanoTime();
        keystoreUpdatedTimeSecretKeyPath = getString(ConfigKeys.KEYSTORE_UPDATED_TIMESTAMP_PATH,
                                                     KEYSTORE_UPDATED_TIMESTAMP_PATH_VALUE);
        truststoreLastUpdatedTime = keystoreLastUpdatedTime;
        truststoreUpdatedTimeSecretKeyPath = getString(ConfigKeys.TRUSTSTORE_UPDATED_TIMESTAMP_PATH,
                                                       TRUSTSTORE_UPDATED_TIMESTAMP_PATH_VALUE);
    }

    @Override
    public synchronized void initHotReloading() {
        // No-op
    }

    /**
     * Checks mounted paths for {@code KEYSTORE_UPDATED_TIMESTAMP_PATH} and {@code TRUSTSTORE_UPDATED_TIMESTAMP_PATH}
     * and compares the values for those variables with the current timestamps. In case the mounted paths are
     * not valid (either they are not initialized yet, got removed or got corrupted in-flight), this method considers
     * that nothing has changed.
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
        logger.info("Comparing keystore timestamps oldValue {} and newValue {}", keystoreLastUpdatedTime,
                    keystoreUpdatedTime);
        if (keystoreUpdatedTime > keystoreLastUpdatedTime) {
            logger.info("Updating the keystoreLastUpdatedTime from oldValue {} to newValue {}",
                        keystoreLastUpdatedTime, keystoreUpdatedTime);
            keystoreLastUpdatedTime = keystoreUpdatedTime;
            return true;
        } else {
            logger.info("Based on the comparision, no keystore update needed");
            return false;
        }
    }

    private boolean hasTruststoreUpdated() {
        long truststoreUpdatedTime = getTruststoreLastUpdatedTime();
        logger.info("Comparing truststore timestamps oldValue {} and newValue {}", truststoreLastUpdatedTime,
                    truststoreUpdatedTime);
        if (truststoreUpdatedTime > truststoreLastUpdatedTime) {
            logger.info("Updating the truststoreLastUpdatedTime from oldValue {} to newValue {}",
                        truststoreLastUpdatedTime, truststoreUpdatedTime);
            truststoreLastUpdatedTime = truststoreUpdatedTime;
            return true;
        } else {
            logger.info("Based on the comparision, no truststore update needed");
            return false;
        }
    }

    private long getKeystoreLastUpdatedTime() {
        Optional<String> keystoreUpdatedTimeSecretKeyValue = readSecretFromMountedVolume(keystoreUpdatedTimeSecretKeyPath);
        if (keystoreUpdatedTimeSecretKeyValue.isPresent())
        {
            return parseLastUpdatedTime(keystoreUpdatedTimeSecretKeyValue.get(), keystoreLastUpdatedTime);
        }
        else
        {
            logger.warn("Failed to load {}'s value. Will use existing value {}", keystoreUpdatedTimeSecretKeyPath,
                        keystoreLastUpdatedTime);
            return keystoreLastUpdatedTime;
        }
    }

    private long getTruststoreLastUpdatedTime() {
        Optional<String> truststoreUpdatedTimeSecretKeyValue = readSecretFromMountedVolume(truststoreUpdatedTimeSecretKeyPath);
        if (truststoreUpdatedTimeSecretKeyValue.isPresent())
        {
            return parseLastUpdatedTime(truststoreUpdatedTimeSecretKeyValue.get(), truststoreLastUpdatedTime);
        }
        else
        {
            logger.warn("Failed to load {}'s value. Will use existing value {}", truststoreUpdatedTimeSecretKeyPath,
                        truststoreLastUpdatedTime);
            return truststoreLastUpdatedTime;
        }
    }

    private Optional<String> readSecretFromMountedVolume(String secretKeyPath) {
        try
        {
            return Optional.of(new String(Files.readAllBytes(Paths.get(secretKeyPath))));
        }
        catch (IOException e)
        {
            logger.warn(String.format("Failed to read secretKeyPath %s from the mounted volume: %s", secretKeyPath, e.getMessage()));
            return Optional.empty();
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
