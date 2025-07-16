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

package org.apache.cassandra.metrics;

public class AuthMetricsManager extends AbstractMetricsManager<String, AuthMetrics>
{
    public final static AuthMetricsManager instance = new AuthMetricsManager(false);

    protected AuthMetricsManager(boolean asyncRegistration)
    {
        super(asyncRegistration);
    }

    @Override
    protected AuthMetrics createMetric(String key) throws IllegalArgumentException
    {
        String[] parts = key.split(",", -1);
        if (parts.length != 3)
            throw new IllegalArgumentException("Invalid key for AuthMetrics: expected 3 parts but got " + parts.length);
        String userName = parts[0];
        boolean authEnabled = Boolean.parseBoolean(parts[1]);
        String authEnforcementFlag = parts[2];
        return new AuthMetrics(userName, authEnabled, authEnforcementFlag);
    }

    @Override
    protected String buildKey(Object... objects) throws IllegalArgumentException
    {
        if (objects.length != 3)
            throw new IllegalArgumentException("Expected 3 arguments: userName (String), authEnabled (Boolean), and authEnforcementFlag (String)");
        return objects[0] + "," + objects[1] + "," + objects[2];
    }

    public static AuthMetrics getMetrics(String userName, boolean authEnabled, String authEnforcementFlag)
    {
        return instance.getMetricsSync(userName, authEnabled, authEnforcementFlag);
    }
}
