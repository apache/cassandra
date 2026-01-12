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
package org.apache.cassandra.service;

import org.apache.cassandra.config.StartupChecksConfiguration;
import org.apache.cassandra.exceptions.StartupException;

/**
 * A test to determine if the system is in a valid state to start up.
 * Some implementations may not actually halt startup, but provide
 * information or advice on tuning and non-fatal environmental issues (e.g. like
 * checking for and warning about suboptimal JVM settings).
 * Other checks may indicate that the system is not in a correct state to be started.
 * Examples include missing or unaccessible data directories, unreadable sstables and
 * misconfiguration of cluster_name in cassandra.yaml.
 *
 * The StartupChecks class manages a collection of these tests, which it executes
 * right at the beginning of the server setup process.
 */
public interface StartupCheck
{
    /**
     * Name of a startup check, as it would appear in the configuration in cassandra.yaml.
     * Not all startup checks are configurable. It is considered to be an illegal state to
     * mention non-configurable startup check in cassandra.yaml.
     *
     * @return name of a startup check
     */
    String name();

    /**
     * Run some test to determine whether the system is safe to be started
     * In the case where a test determines it is not safe to proceed, the
     * test should log a message regarding the reason for the failure and
     * ideally the steps required to remedy the problem.
     *
     * @param configuration all options from descriptor
     * @throws org.apache.cassandra.exceptions.StartupException if the test determines
     * that the environement or system is not in a safe state to startup
     */
    void execute(StartupChecksConfiguration configuration) throws StartupException;

    /**
     * Tells whether a startup check can be configured, at the moment via cassandra.yml.
     *
     * @return true if a startup check is configurable, false otherwise.
     */
    default boolean isConfigurable()
    {
        return false;
    }

    /**
     * Tells if a specific (configurable) check is executed when it is not specified in cassandra.yaml. By default,
     * an implementation of a startup check is executed even if it is not specified. For some checks, it might be
     * preferential to not execute them when they are not explicity mentioned.
     *
     * @return true if a check is not executed when it is not specified in cassandra.yaml, defaults to false - that is,
     * a check will be executed even if it is not explicitly mentioned in cassandra.yaml.
     */
    default boolean isDisabledByDefault()
    {
        return false;
    }

    /**
     * Post-hook after all startup checks succeeded.
     *
     * @param configuration startup check options from descriptor
     */
    default void postAction(StartupChecksConfiguration configuration)
    {
    }
}
