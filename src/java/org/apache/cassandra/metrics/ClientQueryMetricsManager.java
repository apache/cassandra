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

import org.apache.cassandra.utils.Pair;

public class ClientQueryMetricsManager extends AbstractMetricsManager<Pair<String, String>, ClientQueryMetrics>
{
    public static final ClientQueryMetricsManager instance = new ClientQueryMetricsManager();

    @Override
    protected ClientQueryMetrics createMetric(Pair<String, String> key)
    {
        String clientService = key.left;
        String tenancy = key.right;
        return new ClientQueryMetrics(clientService, tenancy);
    }

    @Override
    protected Pair<String, String> buildKey(Object... objects) throws IllegalArgumentException
    {
        if (objects.length != 2)
            throw new IllegalArgumentException("Expected 2 arguments: clientService (String) and tenancy (String)");

        return Pair.create((String) objects[0], (String) objects[1]);
    }

    public static ClientQueryMetrics getQueryMetrics(String clientService, String tenancy)
    {
        return instance.getMetricsSync(clientService, tenancy);
    }
}
