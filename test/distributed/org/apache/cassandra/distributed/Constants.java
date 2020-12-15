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

package org.apache.cassandra.distributed;

public final class Constants
{
    /**
     * Property defined in {@link org.apache.cassandra.distributed.api.IInstanceConfig} which references the ID of the
     * {@link org.apache.cassandra.distributed.api.ICluster}.
     */
    public static final String KEY_DTEST_API_CLUSTER_ID = "dtest.api.cluster_id";

    /**
     * Property used by Instances to determine if checking YAML configuration is required; set to false if validation
     * of the YAML is not desired.
     */
    public static final String KEY_DTEST_API_CONFIG_CHECK = "dtest.api.config.check";

    /**
     * Property used by AbstractCluster to determine how a failed Instance startup state should be; if not set
     * the Instance is marked as "shutdown", but this flag can be used to leave the instance "running" by setting
     * 'true'.
     */
    public static final String KEY_DTEST_API_STARTUP_FAILURE_AS_SHUTDOWN = "dtest.api.startup.failure_as_shutdown";
}
