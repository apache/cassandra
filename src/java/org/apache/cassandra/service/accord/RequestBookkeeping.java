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

package org.apache.cassandra.service.accord;

import java.util.Collections;
import javax.annotation.Nullable;

import accord.primitives.Seekables;
import accord.primitives.TxnId;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.exceptions.ReadFailureException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.RequestFailureException;
import org.apache.cassandra.exceptions.RequestTimeoutException;
import org.apache.cassandra.exceptions.WriteFailureException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.service.accord.exceptions.AccordReadExhaustedException;
import org.apache.cassandra.service.accord.exceptions.AccordReadPreemptedException;
import org.apache.cassandra.service.accord.exceptions.AccordWriteExhaustedException;
import org.apache.cassandra.service.accord.exceptions.AccordWritePreemptedException;

import static org.apache.cassandra.db.ConsistencyLevel.SERIAL;
import static org.apache.cassandra.service.accord.RequestBookkeeping.ThrowsExceptionType.WRITE;

public abstract class RequestBookkeeping
{
    public enum ThrowsExceptionType { READ, WRITE }

    final ThrowsExceptionType exceptionType;

    protected RequestBookkeeping(ThrowsExceptionType exceptionType)
    {
        this.exceptionType = exceptionType;
    }

    abstract void markFailure();
    abstract void markTimeout();
    abstract void markPreempted();
    abstract void markRetryDifferentSystem();
    abstract void markTopologyMismatch();
    abstract void markElapsedNanos(long nanos);

    public RequestTimeoutException newTimeout(@Nullable TxnId txnId, Seekables<?, ?> keysOrRanges)
    {
        markTimeout();
        return newTimeout(txnId, exceptionType, keysOrRanges);
    }

    public RequestTimeoutException newPreempted(@Nullable TxnId txnId, Seekables<?, ?> keysOrRanges)
    {
        markPreempted();
        return newPreempted(txnId, exceptionType, keysOrRanges);
    }

    public RequestTimeoutException newExhausted(@Nullable TxnId txnId, Seekables<?, ?> keysOrRanges)
    {
        markFailure();
        return newExhausted(txnId, exceptionType, keysOrRanges);
    }

    public RequestFailureException newFailed(@Nullable TxnId txnId, Seekables<?, ?> keysOrRanges)
    {
        markFailure();
        return newFailed(txnId, exceptionType, keysOrRanges);
    }

    private static RequestTimeoutException newTimeout(TxnId txnId, ThrowsExceptionType type, Seekables<?, ?> keysOrRanges)
    {
        // Client protocol doesn't handle null consistency level so use ANY
        return type == WRITE ? new WriteTimeoutException(WriteType.CAS, SERIAL, 0, 0, describe(txnId, keysOrRanges))
                             : new ReadTimeoutException(SERIAL, 0, 0, false, describe(txnId, keysOrRanges));
    }

    private static RequestTimeoutException newPreempted(TxnId txnId, ThrowsExceptionType type, Seekables<?, ?> keysOrRanges)
    {
        return type == WRITE ? new AccordWritePreemptedException(0, 0, describe(txnId, keysOrRanges))
                             : new AccordReadPreemptedException(0, 0, false, describe(txnId, keysOrRanges));
    }

    private static RequestTimeoutException newExhausted(TxnId txnId, ThrowsExceptionType type, Seekables<?, ?> keysOrRanges)
    {
        return type == WRITE ? new AccordWriteExhaustedException(0, 0, describe(txnId, keysOrRanges))
                             : new AccordReadExhaustedException(0, 0, false, describe(txnId, keysOrRanges));
    }

    private static RequestFailureException newFailed(TxnId txnId, ThrowsExceptionType type, Seekables<?, ?> keysOrRanges)
    {
        // TODO (required): plumb in per-peer failure responses from Accord, and describe the txnId/keys
        return type == WRITE ? new WriteFailureException(SERIAL, 0, 0, WriteType.CAS, Collections.emptyMap())
                             : new ReadFailureException(SERIAL, 0, 0, false, Collections.emptyMap());
    }

    private static String describe(TxnId txnId, Seekables<?, ?> keysOrRanges)
    {
        return txnId + ": " + keysOrRanges;
    }
}
