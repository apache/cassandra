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

package org.apache.cassandra.service.consensus.migration;

import org.apache.cassandra.service.consensus.TransactionalMode;

/**
 * This tracks the state of a migration either from Paxos -> Accord, Accord [interop mode a] -> Accord [interop mode b] or Accord -> Paxos.
 * The `TransactionalMode` associated with each transition from a system is how interoperability should be achieved during the migration with various performance/safety tradeoffs.
 */
public enum TransactionalMigrationFromMode
{
    none(null),  // No migration is in progress. The currently active transaction system could be either Accord or Paxos.
    off(TransactionalMode.off),
    unsafe(TransactionalMode.unsafe),
    unsafe_writes(TransactionalMode.unsafe_writes),
    mixed_reads(TransactionalMode.mixed_reads),
    full(TransactionalMode.full);

    public final TransactionalMode from;

    TransactionalMigrationFromMode(TransactionalMode from)
    {
        this.from = from;
    }

    public static TransactionalMigrationFromMode fromMode(TransactionalMode prev, TransactionalMode next)
    {
        if (next.accordIsEnabled == prev.accordIsEnabled)
            return none;

        switch (prev)
        {
            default: throw new IllegalArgumentException();
            case off: return off;
            case unsafe: return unsafe;
            case unsafe_writes: return unsafe_writes;
            case mixed_reads: return mixed_reads;
            case full: return full;
        }
    }

    public static TransactionalMigrationFromMode fromOrdinal(int ordinal)
    {
        return values()[ordinal];
    }

    public static TransactionalMigrationFromMode fromString(String name)
    {
        return valueOf(name.toLowerCase());
    }

    public boolean migratingFromAccord()
    {
        return from != null && from.accordIsEnabled;
    }

    public boolean writesThroughAccord()
    {
        return from != null && from.writesThroughAccord;
    }

    public boolean isMigrating()
    {
        return this != none;
    }
}
