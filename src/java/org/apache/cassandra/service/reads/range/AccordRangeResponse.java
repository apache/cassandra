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

package org.apache.cassandra.service.reads.range;

import java.util.function.IntPredicate;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageProxy.ConsensusAttemptResult;
import org.apache.cassandra.service.accord.IAccordService.AsyncTxnResult;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.AbstractIterator;

import static com.google.common.base.Preconditions.checkState;

public class AccordRangeResponse extends AbstractIterator<RowIterator> implements IRangeResponse
{
    private final AsyncTxnResult asyncTxnResult;
    // Range queries don't support reverse, but dutifully threading it through anyways
    private final boolean reversed;
    private final ConsistencyLevel cl;
    private final Dispatcher.RequestTime requestTime;
    private PartitionIterator result;

    public AccordRangeResponse(AsyncTxnResult asyncTxnResult, boolean reversed, ConsistencyLevel cl, Dispatcher.RequestTime requestTime)
    {
        this.asyncTxnResult = asyncTxnResult;
        this.cl = cl;
        this.requestTime = requestTime;
        this.reversed = reversed;
    }

    private void waitForResponse()
    {
        if (result != null)
            return;
        IntPredicate alwaysTrue = ignored -> true;
        IntPredicate alwaysFalse = ignored -> false;
        // TODO (required): Handle retry on different system
        ConsensusAttemptResult consensusAttemptResult = StorageProxy.getConsensusAttemptResultFromAsyncTxnResult(asyncTxnResult, 1, reversed ? alwaysTrue : alwaysFalse, cl, requestTime);
        checkState(!consensusAttemptResult.shouldRetryOnNewConsensusProtocol, "Live migration is not supported yet");
        result = consensusAttemptResult.serialReadResult;
    }

    @Override
    protected RowIterator computeNext()
    {
        waitForResponse();
        return result.hasNext() ? result.next() : endOfData();
    }

    @Override
    public void close()
    {
        // It's an in-memory iterator so no need to close whatever might end up in TxnResult
    }
}
