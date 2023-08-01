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

package org.apache.cassandra.simulator.cluster;

public interface ClusterActionListener
{
    interface TopologyChangeValidator
    {
        public void before(Topology before, int[] participatingKeys);
        public void after(Topology after);
    }

    interface RepairValidator
    {
        public void before(Topology topology, boolean repairPaxos, boolean repairOnlyPaxos);
        public void after();
    }

    TopologyChangeValidator newTopologyChangeValidator(Object id);
    RepairValidator newRepairValidator(Object id);

    class NoOpListener implements ClusterActionListener
    {

        @Override
        public TopologyChangeValidator newTopologyChangeValidator(Object id)
        {
            return new TopologyChangeValidator()
            {
                @Override
                public void before(Topology before, int[] participatingKeys)
                {
                }

                @Override
                public void after(Topology after)
                {
                }
            };
        }

        @Override
        public RepairValidator newRepairValidator(Object id)
        {
            return new RepairValidator()
            {
                @Override
                public void before(Topology topology, boolean repairPaxos, boolean repairOnlyPaxos)
                {
                }

                @Override
                public void after()
                {
                }
            };
        }
    }
}
