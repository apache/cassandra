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

package org.apache.cassandra.service.consensus;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.service.accord.IAccordService;

/*
 * Configure the transactional behavior of a table. Enables accord on a table and defines how it mixes with non-serial writes
 *
 * For Accord transactions to function correctly when mixed with non-SERIAL writes it's necessary for the writes to occur through Accord.
 *
 * Accord will also use this configuration to determine what consistency level to perform its reads
 * at since it will need to be able to read data written at non-SERIAL consistency levels.
 *
 * BlockingReadRepair will also use this configuration to determine how BRR mutations are applied. For migration
 * and accord the BRR mutations will be applied as Accord transactions so that BRR doesn't expose Accord to
 * uncommitted Accord data that is being RRed. This can occur when Accord has applied a transaction at some, but not
 * all replica since Accord defaults to asynchronous commit.
 *
 * By routing repairs through Accord it is guaranteed that the Accord derived contents of the repair have already been applied at any
 * replica where Accord applies the transaction. This also prevents BRR from breaking atomicity of Accord writes.
 *
 * If they are not written through Accord then reads through Accord will be required to occur at
 * consistency level compatible with the non-serial writes preventing single replica reads from being performed
 * by Accord. It will also require Accord to perform read repair of non-serial writes.
 *
 * Even then there is the potential for Accord to inconsistently execute transactions at different replicas
 * because different coordinators for an Accord transaction may encounter different non-SERIAL write state and
 * race to commit different outcomes for the transaction.
 *
 * This is different from Paxos because Paxos performs consensus on the actual values to be applied so recovery
 * coordinators will always produce a consistent state when applying a transaction. Accord performs consensus on
 * the execution order of transaction and different coordinators witnessing different states not managed by Accord
 * can produce multiple outcomes for a transaction.
 *
 * // TODO to safely migrate you would have to route all writes through Accord with the current implementation
 * // We could do it by range instead in the migration version, but then we need to know when all in flight writes
 * // are done before marking a range as migrated. Would waiting out the timeout be enough (timeout bugs!)?
 */
public enum TransactionalMode
{
    // Running on Paxos V1 or V2 with Accord disabled
    off(false, false, false, false),

    /*
     * Execute writes through Cassandra via StorageProxy's normal write path. This can lead Accord to compute
     * multiple outcomes for a transaction that depends on data written by non-SERIAL writes.
     */
    unsafe(true, false, false, false),

    /*
     * Allow mixing of non-SERIAL writes and Accord, but still force BRR through Accord.
     * This mode makes it safe to perform non-SERIAL or SERIAL reads of Accord data, but unsafe
     * to write data that Accord may attempt to read.
     */
    unsafe_writes(true, false, false, true),

    /*
     * Execute writes through Accord skipping StorageProxy's normal write path, but commit
     * writes at the provided consistency level so they can be read via non-SERIAL consistency levels.
     * This mode makes it safe to read/write data that Accord will read/write.
     */
    mixed_reads(true, false, true, true),

    /*
     * Execute writes through Accord skipping StorageProxy's normal write path. Ignores the provided consistency level
     * which makes Accord commit writes at ANY similar to Paxos with commit consistency level ANY.
     */
    full(true, true, true, true);

    public final boolean accordIsEnabled;
    public final boolean ignoresSuppliedConsistencyLevel;
    public final boolean writesThroughAccord;

    public final boolean blockingReadRepairThroughAccord;
    private final String cqlParam;

    TransactionalMode(boolean accordIsEnabled, boolean ignoresSuppliedConsistencyLevel, boolean writesThroughAccord, boolean blockingReadRepairThroughAccord)
    {
        this.accordIsEnabled = accordIsEnabled;
        this.ignoresSuppliedConsistencyLevel = ignoresSuppliedConsistencyLevel;
        this.writesThroughAccord = writesThroughAccord;
        this.blockingReadRepairThroughAccord = blockingReadRepairThroughAccord;
        this.cqlParam = String.format("transactional_mode = '%s'", this.name().toLowerCase());
    }

    public ConsistencyLevel commitCLForStrategy(ConsistencyLevel consistencyLevel)
    {
        if (ignoresSuppliedConsistencyLevel)
            return null;

        if (!IAccordService.SUPPORTED_COMMIT_CONSISTENCY_LEVELS.contains(consistencyLevel))
            throw new UnsupportedOperationException("Consistency level " + consistencyLevel + " is unsupported with Accord for write/commit, supported are ANY, ONE, QUORUM, and ALL");

        return consistencyLevel;
    }

    public ConsistencyLevel readCLForStrategy(ConsistencyLevel consistencyLevel)
    {
        if (ignoresSuppliedConsistencyLevel)
            return null;

        if (!IAccordService.SUPPORTED_READ_CONSISTENCY_LEVELS.contains(consistencyLevel))
            throw new UnsupportedOperationException("Consistency level " + consistencyLevel + " is unsupported with Accord for read, supported are ONE, QUORUM, and SERIAL");

        return consistencyLevel;
    }

    public String asCqlParam()
    {
        return cqlParam;
    }

    public static TransactionalMode fromOrdinal(int ordinal)
    {
        return values()[ordinal];
    }

    public static TransactionalMode fromString(String name)
    {
        return valueOf(name.toLowerCase());
    }
}
