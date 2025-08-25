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

import org.apache.cassandra.repair.autorepair.AutoRepairConfig.RepairType;

public class AutoRepairMetricsManager extends AbstractMetricsManager<RepairType, AutoRepairMetrics>
{
    public final static AutoRepairMetricsManager instance = new AutoRepairMetricsManager(false);

    protected AutoRepairMetricsManager(boolean asyncRegistration)
    {
        super(asyncRegistration);
    }

    @Override
    protected AutoRepairMetrics createMetric(RepairType repairType)
    {
        return new AutoRepairMetrics(repairType);
    }

    @Override
    protected RepairType buildKey(Object... objects) throws IllegalArgumentException
    {
        if (objects.length != 1 || !(objects[0] instanceof RepairType))
            throw new IllegalArgumentException("Expected a single RepairType argument");

        return ((RepairType) objects[0]);
    }

    public static AutoRepairMetrics getMetrics(RepairType repairType)
    {
        return instance.getMetricsSync(repairType);
    }
}
