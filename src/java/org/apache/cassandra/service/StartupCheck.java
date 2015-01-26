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

import org.apache.cassandra.exceptions.StartupException;

/**
 * A test to determine if the system is in a valid state to start up.
 * Some implementations may not actually halt startup, but provide
 * information or advice on tuning and non-fatal environmental issues (e.g. like
 * checking for and warning about suboptimal JVM settings).
 * Other checks may indicate that they system is not in a correct state to be started.
 * Examples include inability to load JNA when the cassandra.boot_without_jna option
 * is not set, missing or unaccessible data directories, unreadable sstables and
 * misconfiguration of cluster_name in cassandra.yaml.
 *
 * The StartupChecks class manages a collection of these tests, which it executes
 * right at the beginning of the server settup process.
 */
public interface StartupCheck
{
    /**
     * Run some test to determine whether the system is safe to be started
     * In the case where a test determines it is not safe to proceed, the
     * test should log a message regarding the reason for the failure and
     * ideally the steps required to remedy the problem.
     *
     * @throws org.apache.cassandra.exceptions.StartupException if the test determines
     * that the environement or system is not in a safe state to startup
     */
    void execute() throws StartupException;
}
