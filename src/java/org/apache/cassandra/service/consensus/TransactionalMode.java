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

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.dht.NormalizedRanges;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.IAccordService;
import org.apache.cassandra.service.consensus.migration.TableMigrationState;
import org.apache.cassandra.service.consensus.migration.TransactionalMigrationFromMode;
import org.apache.cassandra.tcm.ClusterMetadata;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.cassandra.dht.NormalizedRanges.normalizedRanges;

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
    off(false, false, false, false, false),

    // TODO (maybe): These unsafe modes don't have Accord do async commit and single replica reads so how useful are they besides preserving non-SERIAL performance?
    // These modes are unsafe when Accord and non-SERIAL reads and writes interact with the same data
    // They don't guarantee that non-SERIAL reads or writes will see the latest Accord writes or that
    // Accord transactions will recover correctly

    /*
     * Enables Accord but does not allow non-SERIAL reads and writes to occur safely to data read/written by Accord
     *
     * Execute non-SERIAL writes through Cassandra via StorageProxy's normal write path. This can lead Accord to compute
     * multiple outcomes for a transaction that depend on data written by non-SERIAL writes.
     *
     * SERIAL reads and CAS will run on Accord. Accord will honor provided consistency levels and do synchronous commit
     * so the results can be read correctly with non-SERIAL CLs, but read repair could interfere with Accord.
     */
    unsafe(true, false, false, false, false),

    /*
     * Enables Accord and makes it safe to perform non-SERIAL reads of Accord data without guaranteeing that they will
     * see the latest Accord writes. non-SERIAL writes to data read by Accord will make Accord txn recovery non-deterministic
     *
     * Allow mixing of non-SERIAL writes and Accord, but still force BRR through Accord.
     * This mode makes it safe to perform non-SERIAL or SERIAL reads of Accord data, but unsafe
     * to write data that Accord may attempt to read.
     */
    unsafe_writes(true, false, false, false, true),

    // These modes always provide correct execution with mixed_reads allow non-transaction non-SERIAL operations
    // at the expense of slower Accord transactions, and full allowing faster transaction execution, but forcing
    // all reads and writes to occur transactionally

    /*
     * Execute writes through Accord skipping StorageProxy's normal write path, but commit
     * writes at the provided consistency level so they can be read via non-SERIAL consistency levels.
     * This mode makes it safe to read/write data that Accord will read/write.
     */
    mixed_reads(true, false, true, false, true),

    /*
     * Execute writes through Accord skipping StorageProxy's normal write path. Ignores the provided consistency level
     * which makes Accord commit writes at ANY similar to Paxos with commit consistency level ANY.
     */
    full(true, true, true, true, true);

    public final boolean accordIsEnabled;
    public final boolean ignoresSuppliedCommitCL;
    public final boolean nonSerialWritesThroughAccord;
    public final boolean nonSerialReadsThroughAccord;
    public final boolean blockingReadRepairThroughAccord;
    private final String cqlParam;

    TransactionalMode(boolean accordIsEnabled, boolean ignoreSuppliedCommitCL, boolean nonSerialWritesThroughAccord, boolean nonSerialReadsThroughAccord, boolean blockingReadRepairThroughAccord)
    {
        this.accordIsEnabled = accordIsEnabled;
        this.ignoresSuppliedCommitCL = ignoreSuppliedCommitCL;
        this.nonSerialWritesThroughAccord = nonSerialWritesThroughAccord;
        this.nonSerialReadsThroughAccord = nonSerialReadsThroughAccord;
        this.blockingReadRepairThroughAccord = blockingReadRepairThroughAccord;
        this.cqlParam = String.format("transactional_mode = '%s'", this.name().toLowerCase());
    }

    public ConsistencyLevel commitCLForStrategy(TransactionalMigrationFromMode fromMode, ConsistencyLevel consistencyLevel, ClusterMetadata cm, TableId tableId, Token token)
    {
        if (ignoresSuppliedCommitCL)
        {
            TableMigrationState tms = cm.consensusMigrationState.tableStates.get(tableId);
            checkState(tms != null || fromMode == TransactionalMigrationFromMode.none);

            // Only ignore the supplied consistency level if the token is not migrating
            // otherwise honor it since there could still be Paxos and non-SERIAL reads racing with migration.
            // Migrating to Accord, Paxos continues reading during the first phase of migration
            // Migrating to Paxos, this doesn't really matter since this transaction will get RetryOnDifferentSystemException
            if (tms == null || tms.migratedRanges.intersects(token))
                return null;
        }

        if (!IAccordService.SUPPORTED_COMMIT_CONSISTENCY_LEVELS.contains(consistencyLevel))
            throw new UnsupportedOperationException("Consistency level " + consistencyLevel + " is unsupported with Accord for write/commit, supported are ANY, ONE, QUORUM, and ALL");

        return consistencyLevel;
    }

    /**
     * Infer whether Accord can ignore the read CL and bias towards correctness by reading from a quorum
     * if it's needed due to how non-SERIAL writes are done
     */
    private boolean ignoresSuppliedReadCL()
    {
        return nonSerialWritesThroughAccord && blockingReadRepairThroughAccord;
    }


    public ConsistencyLevel readCLForStrategy(TransactionalMigrationFromMode fromMode, ConsistencyLevel consistencyLevel, ClusterMetadata cm, TableId tableId, Token token)
    {
        if (ignoresSuppliedReadCL())
        {
            TableMigrationState tms = cm.consensusMigrationState.tableStates.get(tableId);
            checkState(tms != null || fromMode == TransactionalMigrationFromMode.none);

            // Only ignore the supplied consistency level if the token is not migrating
            // otherwise honor it because we might read through Accord for non-SERIAL reads before repair is run
            // this is OK to do because BRR still works and Accord isn't computing a write so recovery
            // determinism isn't an issue
            if (tms == null || tms.migratedRanges.intersects(token))
                return null;
        }

        if (!IAccordService.SUPPORTED_READ_CONSISTENCY_LEVELS.contains(consistencyLevel))
            throw new UnsupportedOperationException("Consistency level " + consistencyLevel + " is unsupported with Accord for read, supported are ONE, QUORUM, and SERIAL");

        return consistencyLevel;
    }

    public ConsistencyLevel readCLForStrategy(TransactionalMigrationFromMode fromMode, ConsistencyLevel consistencyLevel, ClusterMetadata cm, TableId tableId, Range<Token> range)
    {
        if (ignoresSuppliedReadCL())
        {
            TableMigrationState tms = cm.consensusMigrationState.tableStates.get(tableId);
            checkState(tms != null || fromMode == TransactionalMigrationFromMode.none);

            NormalizedRanges<Token> ranges = normalizedRanges(ImmutableList.of(range));
            // Only ignore the supplied consistency level if none of the range is migrating
            // otherwise honor it because we might read through Accord for non-SERIAL reads before repair is run
            // this is OK to do because BRR still works and Accord isn't computing a write so recovery
            // determinism isn't an issue
            if (tms == null || tms.migratedRanges.intersection(ranges).equals(ranges))
                return null;
        }

        if (!IAccordService.SUPPORTED_READ_CONSISTENCY_LEVELS.contains(consistencyLevel))
            throw new UnsupportedOperationException("Consistency level " + consistencyLevel + " is unsupported with Accord for read, supported are ONE, QUORUM, and SERIAL");
        return consistencyLevel;
    }

    public String asCqlParam()
    {
        return cqlParam;
    }

    public boolean nonSerialWritesThroughAccord()
    {
        return nonSerialWritesThroughAccord;
    }

    public boolean readRepairsThroughAccord()
    {
        return blockingReadRepairThroughAccord;
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
