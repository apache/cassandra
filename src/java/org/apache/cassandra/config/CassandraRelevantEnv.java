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

package org.apache.cassandra.config;

// checkstyle: suppress below 'blockSystemPropertyUsage'

import java.util.Arrays;
import java.util.Optional;

import org.apache.cassandra.exceptions.ConfigurationException;

import static org.apache.cassandra.utils.LocalizeString.toUpperCaseLocalized;

public enum CassandraRelevantEnv
{
    /**
     * Searching in the JAVA_HOME is safer than searching into System.getProperty("java.home") as the Oracle
     * JVM might use the JRE which do not contains jmap.
     */
    JAVA_HOME ("JAVA_HOME"),
    CIRCLECI("CIRCLECI"),
    CASSANDRA_SKIP_SYNC("CASSANDRA_SKIP_SYNC"),
    /** By default, the standard Cassandra CLI layout is used for backward compatibility, however,
     * the new Picocli layout can be enabled by setting this property to the {@code "picocli"}. */
    CASSANDRA_CLI_LAYOUT("CASSANDRA_CLI_LAYOUT"),
    /**
     * Allow overriding
     */
    CASSANDRA_ALLOW_CONFIG_ENVIRONMENT_VARIABLES("CASSANDRA_ALLOW_CONFIG_ENVIRONMENT_VARIABLES")
    ;

    CassandraRelevantEnv(String key)
    {
        this.key = key;
    }

    private final String key;

    public String getString()
    {
        return System.getenv(key);
    }

    /**
     * Gets the value of a system env as a boolean.
     * @return System env boolean value if it exists, false otherwise.
     */
    public boolean getBoolean()
    {
        return Boolean.parseBoolean(System.getenv(key));
    }

    public boolean getBooleanOrDefault(boolean defaultValue)
    {
        return Optional.ofNullable(System.getenv(key)).map(Boolean::parseBoolean).orElse(defaultValue);
    }

    public String getKey() {
        return key;
    }

    public <T extends Enum<T>> T getEnum(boolean toUppercase, Class<T> enumClass, String defaultVal)
    {
        String value = System.getenv(key);
        value = value == null ? defaultVal : value;
        try
        {
            return Enum.valueOf(enumClass, toUppercase ? toUpperCaseLocalized(value) : value);
        }
        catch (IllegalArgumentException e)
        {
            throw new ConfigurationException(String.format("Invalid value for environment variable '%s': " +
                                                           "expected one of %s (case-insensitive) but was '%s'",
                                                           key, Arrays.toString(enumClass.getEnumConstants()), value));
        }
    }
}
