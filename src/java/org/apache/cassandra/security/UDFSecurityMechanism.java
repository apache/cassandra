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

import java.util.Locale;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.LocalizeString;
import org.apache.cassandra.utils.logging.LoggingSupportFactory;

import static org.apache.cassandra.config.CassandraRelevantProperties.UDF_SECURITY_MECHANISM;

/**
 * The Java UDF sandbox, selected with {@code -Dcassandra.udf.security_mechanism}. {@link #SECURITY_MANAGER}, the
 * default up to Java 23, installs {@link ThreadAwareSecurityManager}. {@link #BYTECODE}, the default from Java 24,
 * checks each compiled UDF against {@code UDFSandboxPolicy} instead.
 */
public enum UDFSecurityMechanism
{
    SECURITY_MANAGER("securitymanager"),
    BYTECODE("bytecode");

    private static final Logger logger = LoggerFactory.getLogger(UDFSecurityMechanism.class);

    static final int FIRST_JDK_WITHOUT_SECURITY_MANAGER = 24;
    static final String AUTO = "auto";

    private static volatile UDFSecurityMechanism current;
    private static boolean installed;

    public final String value;

    UDFSecurityMechanism(String value)
    {
        this.value = value;
    }

    /**
     * @param configured the value of the system property, or null if it is not set; {@code auto} is the same as null
     * @param javaFeatureVersion the running Java feature version
     * @throws ConfigurationException for an unknown value, or for {@code securitymanager} on Java 24 or later
     */
    @VisibleForTesting
    static UDFSecurityMechanism resolve(@Nullable String configured, int javaFeatureVersion)
    {
        boolean noSecurityManager = javaFeatureVersion >= FIRST_JDK_WITHOUT_SECURITY_MANAGER;
        String normalized = normalize(configured);
        if (AUTO.equals(normalized))
            return noSecurityManager ? BYTECODE : SECURITY_MANAGER;

        for (UDFSecurityMechanism mechanism : values())
        {
            if (!mechanism.value.equals(normalized))
                continue;
            if (mechanism == SECURITY_MANAGER && noSecurityManager)
                throw new ConfigurationException(String.format("%s=%s is not supported on Java %d: the Security Manager was removed in Java %d. " +
                                                               "Remove the property or set it to %s.",
                                                               UDF_SECURITY_MECHANISM.getKey(), SECURITY_MANAGER.value, javaFeatureVersion,
                                                               FIRST_JDK_WITHOUT_SECURITY_MANAGER, BYTECODE.value), false);
            return mechanism;
        }
        throw new ConfigurationException(String.format("Invalid value '%s' for %s; expected %s, %s or %s",
                                                       configured, UDF_SECURITY_MECHANISM.getKey(), AUTO, SECURITY_MANAGER.value, BYTECODE.value), false);
    }

    private static String normalize(@Nullable String configured)
    {
        return configured == null ? AUTO : LocalizeString.toLowerCaseLocalized(configured.trim(), Locale.ROOT);
    }

    /**
     * @return the mechanism of this JVM, resolved on first use from the system property and the Java version
     * @throws ConfigurationException if the property is invalid for this JVM
     */
    public static UDFSecurityMechanism current()
    {
        UDFSecurityMechanism mechanism = current;
        if (mechanism == null)
        {
            synchronized (UDFSecurityMechanism.class)
            {
                mechanism = current;
                if (mechanism == null)
                {
                    mechanism = resolve(UDF_SECURITY_MECHANISM.getString(), Runtime.version().feature());
                    current = mechanism;
                }
            }
        }
        return mechanism;
    }

    /**
     * Installs the selected sandbox and runs the logging startup checks. Later calls do nothing.
     */
    public static synchronized void install()
    {
        if (installed)
            return;
        install(current(), ThreadAwareSecurityManager::install);
        LoggingSupportFactory.getLoggingSupport().onStartup();
        installed = true;
    }

    @VisibleForTesting
    static void install(UDFSecurityMechanism mechanism, Runnable securityManagerInstaller)
    {
        if (mechanism == SECURITY_MANAGER)
        {
            try
            {
                securityManagerInstaller.run();
            }
            catch (UnsupportedOperationException | SecurityException e)
            {
                // thrown on Java 18 and later without -Djava.security.manager=allow, or when another Security Manager
                // refuses to be replaced
                throw new ConfigurationException(String.format("Could not install the Security Manager (%s). Add -Djava.security.manager=allow " +
                                                               "or set %s=%s.", e, UDF_SECURITY_MECHANISM.getKey(), BYTECODE.value), e);
            }
        }
        if (AUTO.equals(normalize(UDF_SECURITY_MECHANISM.getString())))
            logger.info("Using the {} UDF sandbox, the default on Java {}", mechanism.value, Runtime.version().feature());
        else
            logger.info("Using the {} UDF sandbox", mechanism.value);
    }
}
