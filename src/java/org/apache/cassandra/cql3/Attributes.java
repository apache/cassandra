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
import java.util.Collections;

import com.google.common.collect.Iterables;

import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Utility class for the Parser to gather attributes for modification
 * statements.
 */
public class Attributes
{
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

    public Iterable<Function> getFunctions()
    {
        if (timestamp != null && timeToLive != null)
            return Iterables.concat(timestamp.getFunctions(), timeToLive.getFunctions());
        else if (timestamp != null)
            return timestamp.getFunctions();
        else if (timeToLive != null)
            return timeToLive.getFunctions();
        else
            return Collections.emptySet();
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

    public int getTimeToLive(QueryOptions options, int defaultTimeToLive) throws InvalidRequestException
    {
        if (timeToLive == null)
            return defaultTimeToLive;

        ByteBuffer tval = timeToLive.bindAndGet(options);
        if (tval == null)
            throw new InvalidRequestException("Invalid null value of TTL");

        if (tval == ByteBufferUtil.UNSET_BYTE_BUFFER)
            return defaultTimeToLive;

        try
        {
            Int32Type.instance.validate(tval);
        }
        catch (MarshalException e)
        {
            throw new InvalidRequestException("Invalid timestamp value: " + tval);
        }

        int ttl = Int32Type.instance.compose(tval);
        if (ttl < 0)
            throw new InvalidRequestException("A TTL must be greater or equal to 0, but was " + ttl);

        if (ttl > MAX_TTL)
            throw new InvalidRequestException(String.format("ttl is too large. requested (%d) maximum (%d)", ttl, MAX_TTL));

        if (defaultTimeToLive != LivenessInfo.NO_TTL && ttl == LivenessInfo.NO_TTL)
            return LivenessInfo.NO_TTL;

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
}
