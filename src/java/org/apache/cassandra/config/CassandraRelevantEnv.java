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

public enum CassandraRelevantEnv
{
    /**
     * Searching in the JAVA_HOME is safer than searching into System.getProperty("java.home") as the Oracle
     * JVM might use the JRE which do not contains jmap.
     */
    JAVA_HOME ("JAVA_HOME"),
    CIRCLECI("CIRCLECI"),
    CASSANDRA_SKIP_SYNC("CASSANDRA_SKIP_SYNC")

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

    public String getKey() {
        return key;
    }
}
