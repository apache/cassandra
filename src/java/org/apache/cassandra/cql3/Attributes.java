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
package org.apache.cassandra.cql3;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.db.ExpirationDateOverflowHandling;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * Utility class for the Parser to gather attributes for modification
 * statements.
 */
public class Attributes
{
    /**
     * If this limit is ever raised, make sure @{@link Integer#MAX_VALUE} is not allowed,
     * as this is used as a flag to represent expired liveness.
     *
     * See {@link org.apache.cassandra.db.LivenessInfo#EXPIRED_LIVENESS_TTL}
     */
    public static final int MAX_TTL = 20 * 365 * 24 * 60 * 60; // 20 years in seconds

    private final Term timestamp;
    private final Term timeToLive;

    public static Attributes none()
    {
        return new Attributes(null, null);
    }

    private Attributes(Term timestamp, Term timeToLive)
    {
        this.timestamp = timestamp;
        this.timeToLive = timeToLive;
    }

    public void addFunctionsTo(List<Function> functions)
    {
        if (timestamp != null)
            timestamp.addFunctionsTo(functions);
        if (timeToLive != null)
            timeToLive.addFunctionsTo(functions);
    }

    public boolean isTimestampSet()
    {
        return timestamp != null;
    }

    public boolean isTimeToLiveSet()
    {
        return timeToLive != null;
    }

    public long getTimestamp(long now, QueryOptions options) throws InvalidRequestException
    {
        if (timestamp == null)
            return now;

        ByteBuffer tval = timestamp.bindAndGet(options);
        if (tval == null)
            throw new InvalidRequestException("Invalid null value of timestamp");

        if (tval == ByteBufferUtil.UNSET_BYTE_BUFFER)
            return now;

        try
        {
            LongType.instance.validate(tval);
        }
        catch (MarshalException e)
        {
            throw new InvalidRequestException("Invalid timestamp value: " + tval);
        }

        return LongType.instance.compose(tval);
    }

    public int getTimeToLive(QueryOptions options, TableMetadata metadata) throws InvalidRequestException
    {
        if (timeToLive == null)
        {
            ExpirationDateOverflowHandling.maybeApplyExpirationDateOverflowPolicy(metadata, metadata.params.defaultTimeToLive, true);
            return metadata.params.defaultTimeToLive;
        }

        ByteBuffer tval = timeToLive.bindAndGet(options);
        if (tval == null)
            return 0;

        if (tval == ByteBufferUtil.UNSET_BYTE_BUFFER)
            return metadata.params.defaultTimeToLive;

        // byte[0] and null are the same for Int32Type.  UNSET_BYTE_BUFFER is also byte[0] but we rely on pointer
        // identity, so need to check this after checking that
        if (ByteBufferUtil.EMPTY_BYTE_BUFFER.equals(tval))
            return 0;

        try
        {
            Int32Type.instance.validate(tval);
        }
        catch (MarshalException e)
        {
            throw new InvalidRequestException("Invalid TTL value: " + tval);
        }

        int ttl = Int32Type.instance.compose(tval);
        if (ttl < 0)
            throw new InvalidRequestException("A TTL must be greater or equal to 0, but was " + ttl);

        if (ttl > MAX_TTL)
            throw new InvalidRequestException(String.format("ttl is too large. requested (%d) maximum (%d)", ttl, MAX_TTL));

        if (metadata.params.defaultTimeToLive != LivenessInfo.NO_TTL && ttl == LivenessInfo.NO_TTL)
            return LivenessInfo.NO_TTL;

        ExpirationDateOverflowHandling.maybeApplyExpirationDateOverflowPolicy(metadata, ttl, false);

        return ttl;
    }

    public void collectMarkerSpecification(VariableSpecifications boundNames)
    {
        if (timestamp != null)
            timestamp.collectMarkerSpecification(boundNames);
        if (timeToLive != null)
            timeToLive.collectMarkerSpecification(boundNames);
    }

    public static class Raw
    {
        public Term.Raw timestamp;
        public Term.Raw timeToLive;

        public Attributes prepare(String ksName, String cfName) throws InvalidRequestException
        {
            Term ts = timestamp == null ? null : timestamp.prepare(ksName, timestampReceiver(ksName, cfName));
            Term ttl = timeToLive == null ? null : timeToLive.prepare(ksName, timeToLiveReceiver(ksName, cfName));
            return new Attributes(ts, ttl);
        }

        private ColumnSpecification timestampReceiver(String ksName, String cfName)
        {
            return new ColumnSpecification(ksName, cfName, new ColumnIdentifier("[timestamp]", true), LongType.instance);
        }

        private ColumnSpecification timeToLiveReceiver(String ksName, String cfName)
        {
            return new ColumnSpecification(ksName, cfName, new ColumnIdentifier("[ttl]", true), Int32Type.instance);
        }
    }
    
    @Override
    public String toString()
    {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }
}
