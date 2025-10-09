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

import org.apache.cassandra.schema.ReplicationType;

/**
 * Configuration for replication behavior in simulations.
 */
public enum ReplicationConfig
{
    /**
     * Standard untracked replication - no mutation tracking or witnesses.
     */
    SIMPLE(ReplicationType.untracked, false),

    /**
     * Mutation tracking enabled without witness replicas.
     */
    TRACKING(ReplicationType.tracked, false),

    /**
     * Mutation tracking enabled with witness replicas.
     */
    WITNESSES(ReplicationType.tracked, true);

    private final ReplicationType replicationType;
    private final boolean withWitnesses;

    ReplicationConfig(ReplicationType replicationType, boolean withWitnesses)
    {
        this.replicationType = replicationType;
        this.withWitnesses = withWitnesses;
    }

    public ReplicationType replicationType()
    {
        return replicationType;
    }

    public boolean withWitnesses()
    {
        return withWitnesses;
    }

    public boolean isTracked()
    {
        return replicationType.isTracked();
    }

    /**
     * Calculates the formatted replication factors for each DC.
     * For witnesses mode, uses format 'RF/transientCount' where transient replicas
     * are distributed evenly across DCs while ensuring each DC has at least 1 full replica.
     *
     * @param rfPerDc array of RF per datacenter
     * @return array of formatted strings for each DC (e.g., "'3/1'" for witnesses, "3" for others)
     */
    public String[] formatReplicationFactorsPerDc(int[] rfPerDc)
    {
        if (!withWitnesses)
        {
            // No witnesses - just return RF as strings
            String[] result = new String[rfPerDc.length];
            for (int i = 0; i < rfPerDc.length; i++)
                result[i] = String.valueOf(rfPerDc[i]);
            return result;
        }

        // Calculate witness distribution using the multi-DC algorithm
        int numDcs = rfPerDc.length;
        int totalRf = 0;
        for (int rf : rfPerDc) totalRf += rf;

        int totalFull = totalRf / 2 + 1;
        int[] fullPerDc = new int[numDcs];

        // Each DC gets at least 1 full replica
        for (int i = 0; i < numDcs; i++)
            fullPerDc[i] = 1;

        int remainingFull = totalFull - numDcs;

        // Distribute remaining full replicas to DCs with most remaining capacity
        // Goal: spread witnesses (remaining slots) evenly
        while (remainingFull > 0)
        {
            int maxWitnessesIdx = -1;
            int maxWitnesses = 0;
            for (int i = 0; i < numDcs; i++)
            {
                int witnesses = rfPerDc[i] - fullPerDc[i];
                if (witnesses > maxWitnesses)
                {
                    maxWitnesses = witnesses;
                    maxWitnessesIdx = i;
                }
            }
            if (maxWitnessesIdx >= 0 && maxWitnesses > 0)
            {
                fullPerDc[maxWitnessesIdx]++;
                remainingFull--;
            }
            else
            {
                break;
            }
        }

        // Format as "'RF/witnesses'"
        String[] result = new String[numDcs];
        for (int i = 0; i < numDcs; i++)
        {
            int witnesses = rfPerDc[i] - fullPerDc[i];
            result[i] = "'" + rfPerDc[i] + "/" + witnesses + "'";
        }
        return result;
    }

    /**
     * Returns the CQL clause to append to CREATE KEYSPACE for this replication config.
     * Returns empty string if no special configuration is needed.
     * Note: Witness replicas are configured via the RF format (e.g., '3/1'), not via a keyspace property.
     */
    public String asCqlKeyspaceClause()
    {
        if (replicationType == ReplicationType.untracked)
            return "";

        // Only append replication_type, witnesses are handled via RF format in formatReplicationFactorsPerDc
        return " AND replication_type = '" + replicationType.name() + "'";
    }

    public static ReplicationConfig fromString(String name)
    {
        return valueOf(name.toUpperCase().replace('-', '_'));
    }
}
