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

package org.apache.cassandra.exceptions;

import java.io.IOException;
import java.util.Map;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;

public class QueryReferencesTooManyIndexesAbortException extends ReadAbortException
{
    public final int nodes;
    public final long maxValue;

    public QueryReferencesTooManyIndexesAbortException(String msg, int nodes, long maxValue, boolean dataPresent, ConsistencyLevel consistency, int received, int blockFor, Map<InetAddressAndPort, RequestFailureReason> failureReasonByEndpoint)
    {
        super(msg, consistency, received, blockFor, dataPresent, failureReasonByEndpoint);
        this.nodes = nodes;
        this.maxValue = maxValue;
    }

    @Override
    protected void serializeSpecificFields(DataOutputPlus out, int version) throws IOException
    {
        // Serialize parent fields first
        super.serializeSpecificFields(out, version);
        // Add QueryReferencesTooManyIndexesAbortException specific fields
        out.writeUnsignedVInt32(nodes);
        out.writeUnsignedVInt(maxValue);
    }

    @Override
    protected long serializedSizeSpecificFields(int version)
    {
        return super.serializedSizeSpecificFields(version) +
               TypeSizes.sizeofUnsignedVInt(nodes) +
               TypeSizes.sizeofUnsignedVInt(maxValue);
    }

    static QueryReferencesTooManyIndexesAbortException deserializeFields(String message, DataInputPlus in, int version) throws IOException
    {
        ReadFailureException.DeserializedFields fields = ReadFailureException.deserializeBaseFields(in, version);
        int nodes = in.readUnsignedVInt32();
        long maxValue = in.readUnsignedVInt();

        return new QueryReferencesTooManyIndexesAbortException(message, nodes, maxValue, fields.dataPresent,
                                                               fields.consistency, fields.received, fields.blockFor, fields.failures);
    }

    @Override
    public CassandraExceptionCode getCassandraExceptionCode()
    {
        return CassandraExceptionCode.QUERY_TOO_MANY_INDEXES_ABORT;
    }
}
