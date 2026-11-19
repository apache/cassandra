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

package org.apache.cassandra.service;

/**
 * When running on Accord each cell's timestamp is updated to be the transaction timestamp, but some use cases need a way
 * to opt out of this behavior and preserve the timestamp; examples are hints and batches that have some mutations using
 * the server's timestamp and others using USING TIMESTAMP.
 * <p>
 * The ordinal is serialized as part of {@link org.apache.cassandra.service.accord.txn.TxnUpdate}'s flags, so values
 * must not be reordered, and the low bit must equal {@link #preserve}, since 6.0.0-alpha2 interprets this bit only
 */
public enum AccordTimestamps
{
    /**
     * Timestamps are defined by the transaction executeAt; the update's own times are arbitrary (e.g. assigned by the
     * coordinator's clock) so must be fully overridden
     */
    OVERWRITE(false),

    /**
     * Timestamps are already set e.g. via USING TIMESTAMP, and should not be overwritten.
     * Only generally intended for upgraded mutations. Hints and batchlog replay also declare their original timestamps,
     * so that they do not resurrect data that was deleted after they were first written.
     * This breaks future read detection, so cannot be used for linearizable writes.
     * TODO (required): add tests confirming we do not mix USING TIMESTAMP with TRANSACTION statements,
     *  as logic is insufficient and incompatible with e.g. data store detects future reads behaviour
     * TODO (expected): verify safety - we might need to move detectsFutureReads to epoch/topology state
     */
    DECLARED(true),

    /**
     * Timestamps are defined by the transaction executeAt, and the update's timestamps are normalised
     * (see {@link org.apache.cassandra.service.accord.txn.TxnWrite.Fragment#normaliseBaseUpdateTimes})
     * so that they may be deserialized directly to the correct value
     */
    NORMALISED(false),

    /**
     * When working with a BATCH, different mutations can have different time sources (one mutation might be
     * server based, and another is USING TIMESTAMP based); "mixed" is for this case.
     */
    MIXED(true),
    ;

    private static final AccordTimestamps[] VALUES = values();

    public final boolean preserve;

    AccordTimestamps(boolean preserve)
    {
        this.preserve = preserve;
    }

    public static AccordTimestamps merge(AccordTimestamps left, TimestampSource right)
    {
        if (right == TimestampSource.mixed)
            return MIXED;
        if (left.preserve)
            return left;
        if (right == TimestampSource.using)
            return DECLARED;
        return left;
    }

    public AccordTimestamps merge(TimestampSource right)
    {
        return merge(this, right);
    }

    public static AccordTimestamps forOrdinal(int ordinal)
    {
        return VALUES[ordinal];
    }
}
