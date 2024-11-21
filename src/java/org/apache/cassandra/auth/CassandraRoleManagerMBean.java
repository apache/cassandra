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

package org.apache.cassandra.auth;

/**
 * MBean utilities for dynamic access to CassandraRoleManager
 */
public interface CassandraRoleManagerMBean
{
    /**
     * Get the period between invalid client disconnect attempts
     * @return time between attempts in milliseconds
     */
    long getInvalidClientDisconnectPeriodMillis();

    /**
     * Set the period between invalid client disconnect attempts
     * @param duration time between attempts in milliseconds
     */
    void setInvalidClientDisconnectPeriodMillis(long duration);

    /**
     * Get the maximum jitter between invalid client disconnect attempts
     * @return maximum jitter in milliseconds
     */
    long getInvalidClientDisconnectMaxJitterMillis();

    /**
     * Set the maximum jitter between invalid client disconnect attempts
     * @param duration maximum jitter in milliseconds
     */
    void setInvalidClientDisconnectMaxJitterMillis(long duration);
}
