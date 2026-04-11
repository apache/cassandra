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
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.Sets;

import org.apache.cassandra.db.filter.TombstoneOverwhelmingException;
import org.apache.cassandra.index.IndexBuildInProgressException;
import org.apache.cassandra.index.IndexNotAvailableException;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tcm.NotCMSException;
import org.apache.cassandra.utils.vint.VIntCoding;

import static org.apache.cassandra.net.MessagingService.VERSION_40;

public enum RequestFailureReason
{
    UNKNOWN                                 (0),
    READ_TOO_MANY_TOMBSTONES                (1),
    TIMEOUT                                 (2),
    INCOMPATIBLE_SCHEMA                     (3),
    READ_SIZE                               (4),
    // below reason is only logged, but it does not have associated exception
    NODE_DOWN                               (5),
    INDEX_NOT_AVAILABLE                     (6),
    // below reason does not have an associated exception
    READ_TOO_MANY_INDEXES                   (7),
    NOT_CMS                                 (8),
    INVALID_ROUTING                         (9),
    COORDINATOR_BEHIND                      (10),
    RETRY_ON_DIFFERENT_TRANSACTION_SYSTEM   (11),
    // The following codes have been ported from an external fork, where they were offset explicitly to avoid conflicts.
    INDEX_BUILD_IN_PROGRESS                 (503),
    ;

    static
    {
        // Load RequestFailure class to check that all request failure reasons are handled
        RequestFailure.init();
    }

    public static final Serializer serializer = new Serializer();

    public final int code;

    RequestFailureReason(int code)
    {
        this.code = code;
    }

    private static final Map<Integer, RequestFailureReason> codeToReasonMap = new HashMap<>();
    private static final Map<Class<? extends Throwable>, RequestFailureReason> exceptionToReasonMap = new HashMap<>();

    static
    {
        EnumSet<RequestFailureReason> withoutExceptions = EnumSet.of(UNKNOWN, NODE_DOWN, READ_TOO_MANY_INDEXES);
        Sets.SetView<RequestFailureReason> withExceptions =  Sets.difference(EnumSet.allOf(RequestFailureReason.class), withoutExceptions);
        RequestFailureReason[] reasons = values();

        for (RequestFailureReason reason : reasons)
        {
            if (codeToReasonMap.put(reason.code, reason) != null)
                throw new RuntimeException("Two RequestFailureReason-s that map to the same code: " + reason.code);
        }

        exceptionToReasonMap.put(TombstoneOverwhelmingException.class, READ_TOO_MANY_TOMBSTONES);
        exceptionToReasonMap.put(WriteTimeoutException.class, TIMEOUT);
        exceptionToReasonMap.put(IncompatibleSchemaException.class, INCOMPATIBLE_SCHEMA);
        exceptionToReasonMap.put(ReadSizeAbortException.class, READ_SIZE);
        exceptionToReasonMap.put(IndexNotAvailableException.class, INDEX_NOT_AVAILABLE);
        exceptionToReasonMap.put(NotCMSException.class, NOT_CMS);
        exceptionToReasonMap.put(InvalidRoutingException.class, INVALID_ROUTING);
        exceptionToReasonMap.put(CoordinatorBehindException.class, COORDINATOR_BEHIND);
        exceptionToReasonMap.put(IndexBuildInProgressException.class, INDEX_BUILD_IN_PROGRESS);
        exceptionToReasonMap.put(RetryOnDifferentSystemException.class, RETRY_ON_DIFFERENT_TRANSACTION_SYSTEM);

        if (exceptionToReasonMap.size() != reasons.length - withoutExceptions.size())
        {
            EnumSet<RequestFailureReason> actual = EnumSet.copyOf(exceptionToReasonMap.values());
            Sets.SetView<RequestFailureReason> missing = Sets.difference(withExceptions, actual);
            Sets.SetView<RequestFailureReason> added = Sets.difference(actual, withExceptions);
            StringBuilder sb = new StringBuilder();
            if (!missing.isEmpty())
                sb.append("Expected the following RequestFailureReason, but were missing: ").append(missing).append('\n');
            if (!added.isEmpty())
                sb.append("Unexpected RequestFailureReason found: ").append(added);
            throw new AssertionError(sb.toString());
        }
    }

    public static RequestFailureReason fromCode(int code)
    {
        if (code < 0)
            throw new IllegalArgumentException("RequestFailureReason code must be non-negative (got " + code + ')');

        // be forgiving and return UNKNOWN if we aren't aware of the code - for forward compatibility
        return codeToReasonMap.getOrDefault(code, UNKNOWN);
    }

    public static RequestFailureReason forException(Throwable t)
    {
        RequestFailureReason r = exceptionToReasonMap.get(t.getClass());
        if (r != null)
            return r;

        for (Map.Entry<Class<? extends Throwable>, RequestFailureReason> entry : exceptionToReasonMap.entrySet())
            if (entry.getKey().isInstance(t))
                return entry.getValue();

        return UNKNOWN;
    }

    public static final class Serializer implements IVersionedSerializer<RequestFailureReason>
    {
        private Serializer()
        {
        }

        public void serialize(RequestFailureReason reason, DataOutputPlus out, int version) throws IOException
        {
            assert version >= VERSION_40;
            out.writeUnsignedVInt32(reason.code);
        }

        public RequestFailureReason deserialize(DataInputPlus in, int version) throws IOException
        {
            assert version >= VERSION_40;
            return fromCode(in.readUnsignedVInt32());
        }

        public long serializedSize(RequestFailureReason reason, int version)
        {
            assert version >= VERSION_40;
            return VIntCoding.computeVIntSize(reason.code);
        }
    }
}
