/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.repair;

/**
 * Specify the degree of parallelism when calculating the merkle trees in a repair job.
 */
public enum RepairParallelism
{
    /**
     * One node at a time
     */
    SEQUENTIAL("sequential"),

    /**
     * All nodes at the same time
     */
    PARALLEL("parallel"),

    /**
     * One node per data center at a time
     */
    DATACENTER_AWARE("dc_parallel");

    private final String name;

    /**
     * Return RepairParallelism that match given name.
     * If name is null, or does not match any, this returns default "sequential" parallelism,
     *
     * @param name name of repair parallelism
     * @return RepairParallelism that match given name
     */
    public static RepairParallelism fromName(String name)
    {
        if (PARALLEL.getName().equals(name))
            return PARALLEL;
        else if (DATACENTER_AWARE.getName().equals(name))
            return DATACENTER_AWARE;
        else
            return SEQUENTIAL;
    }

    private RepairParallelism(String name)
    {
        this.name = name;
    }

    public String getName()
    {
        return name;
    }

    @Override
    public String toString()
    {
        return getName();
    }
}
