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
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.cassandra.db.filter.TombstoneOverwhelmingException;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.tcm.NotCMSException;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.cassandra.exceptions.ExceptionSerializer.nullableRemoteExceptionSerializer;

/**
 * Allow inclusion of a serialized exception in failure response messages
 * This continues to use the same verb as the old failure response (whether a message payload or parameter)
 * and has a nullable failure field that may contain a serialized in later versions.
 */
public class RequestFailure
{
    public static final RequestFailure UNKNOWN = new RequestFailure(RequestFailureReason.UNKNOWN);
    public static final RequestFailure READ_TOO_MANY_TOMBSTONES = new RequestFailure(RequestFailureReason.READ_TOO_MANY_TOMBSTONES);
    public static final RequestFailure TIMEOUT = new RequestFailure(RequestFailureReason.TIMEOUT);
    public static final RequestFailure INCOMPATIBLE_SCHEMA = new RequestFailure(RequestFailureReason.INCOMPATIBLE_SCHEMA);
    public static final RequestFailure READ_SIZE = new RequestFailure(RequestFailureReason.READ_SIZE);
    public static final RequestFailure NODE_DOWN = new RequestFailure(RequestFailureReason.NODE_DOWN);
    public static final RequestFailure NOT_CMS = new RequestFailure(RequestFailureReason.NOT_CMS);
    public static final RequestFailure INVALID_ROUTING = new RequestFailure(RequestFailureReason.INVALID_ROUTING);
    public static final RequestFailure INDEX_NOT_AVAILABLE = new RequestFailure(RequestFailureReason.INDEX_NOT_AVAILABLE);
    public static final RequestFailure COORDINATOR_BEHIND = new RequestFailure(RequestFailureReason.COORDINATOR_BEHIND);

    static
    {
        // Validate all reasons are handled
        for (RequestFailureReason reason : RequestFailureReason.values())
            forReason(reason);
    }

    // Allow RequestFailureReason to force class load to check failure reasons are handled
    public static void init() {}

    public static final IVersionedSerializer<RequestFailure> serializer = new IVersionedSerializer<RequestFailure>()
    {
        @Override
        public void serialize(RequestFailure t, DataOutputPlus out, int version) throws IOException
        {
            RequestFailureReason.serializer.serialize(t.reason, out, version);
            if (version >= MessagingService.VERSION_51)
                nullableRemoteExceptionSerializer.serialize(t.failure, out, version);
        }

        @Override
        public RequestFailure deserialize(DataInputPlus in, int version) throws IOException
        {
            RequestFailureReason reason = RequestFailureReason.serializer.deserialize(in, version);
            Throwable failure = null;
            if (version >= MessagingService.VERSION_51)
                failure = nullableRemoteExceptionSerializer.deserialize(in, version);
            if (failure == null)
                return forReason(reason);
            else
                return new RequestFailure(reason, failure);
        }

        @Override
        public long serializedSize(RequestFailure t, int version)
        {
            long size = RequestFailureReason.serializer.serializedSize(t.reason, version);
            if (version >= MessagingService.VERSION_51)
                size += nullableRemoteExceptionSerializer.serializedSize(t.failure, version);
            return size;
        }
    };

    @Nonnull
    public final RequestFailureReason reason;

    @Nullable
    public final Throwable failure;

    public static RequestFailure forException(Throwable t)
    {
        if (t instanceof TombstoneOverwhelmingException)
            return READ_TOO_MANY_TOMBSTONES;

        if (t instanceof IncompatibleSchemaException)
            return INCOMPATIBLE_SCHEMA;

        if (t instanceof NotCMSException)
            return NOT_CMS;

        if (t instanceof InvalidRoutingException)
            return INVALID_ROUTING;

        return UNKNOWN;
    }

    public static RequestFailure forReason(RequestFailureReason reason)
    {
        switch (reason)
        {
            default: throw new IllegalStateException("Unhandled request failure reason " + reason);
            case UNKNOWN: return UNKNOWN;
            case READ_TOO_MANY_TOMBSTONES: return READ_TOO_MANY_TOMBSTONES;
            case TIMEOUT: return TIMEOUT;
            case INCOMPATIBLE_SCHEMA: return INCOMPATIBLE_SCHEMA;
            case READ_SIZE: return READ_SIZE;
            case NODE_DOWN: return NODE_DOWN;
            case NOT_CMS: return NOT_CMS;
            case INVALID_ROUTING: return INVALID_ROUTING;
            case INDEX_NOT_AVAILABLE: return INDEX_NOT_AVAILABLE;
            case COORDINATOR_BEHIND: return COORDINATOR_BEHIND;
        }
    }

    private RequestFailure(RequestFailureReason reason)
    {
        this(reason, null);
    }

    public RequestFailure(@Nonnull Throwable failure)
    {
        this(RequestFailureReason.UNKNOWN, failure);
    }

    public RequestFailure(@Nonnull RequestFailureReason reason, @Nullable Throwable failure)
    {
        checkNotNull(reason);
        this.reason = reason;
        this.failure = failure;
    }

    @Override
    public String toString()
    {
        return "RequestFailure{" +
               "reason=" + reason +
               ", failure='" + failure + '\'' +
               '}';
    }
}
