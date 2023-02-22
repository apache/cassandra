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

package org.apache.cassandra.service.accord.txn;

import java.io.IOException;

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.utils.ObjectSizes;

/**
 * Potentially returned by any transaction that tries to execute in an Epoch
 * where the range has migrated away from Accord
 */
public class RetryWithNewProtocolResult extends TxnResult
{
    private static final long SIZE = ObjectSizes.measure(new RetryWithNewProtocolResult(Epoch.FIRST));

    public final Epoch epoch;

    RetryWithNewProtocolResult(Epoch epoch)
    {
        this.epoch = epoch;
    }

    @Override
    public Kind kind()
    {
        return Kind.retry_new_protocol;
    }

    @Override
    public long estimatedSizeOnHeap()
    {
        return SIZE;
    }

    public static final TxnResultSerializer<RetryWithNewProtocolResult> serializer = new TxnResultSerializer<RetryWithNewProtocolResult>()
    {
        @Override
        public void serialize(RetryWithNewProtocolResult retry, DataOutputPlus out, int version) throws IOException
        {
            Epoch.messageSerializer.serialize(retry.epoch, out, version);
        }

        @Override
        public RetryWithNewProtocolResult deserialize(DataInputPlus in, int version) throws IOException
        {
            return new RetryWithNewProtocolResult(Epoch.messageSerializer.deserialize(in, version));
        }

        @Override
        public long serializedSize(RetryWithNewProtocolResult retry, int version)
        {
            return Epoch.messageSerializer.serializedSize(retry.epoch, version);
        }
    };
}
