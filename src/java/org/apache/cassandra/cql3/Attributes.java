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
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Iterables;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.db.ExpiringCell;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.NoSpamLogger;

/**
 * Utility class for the Parser to gather attributes for modification
 * statements.
 */
public class Attributes
{
    private static final int EXPIRATION_OVERFLOW_WARNING_INTERVAL_MINUTES = Integer.getInteger("cassandra.expiration_overflow_warning_interval_minutes", 5);

    private static final Logger logger = LoggerFactory.getLogger(Attributes.class);

    public enum ExpirationDateOverflowPolicy
    {
        REJECT, CAP
    }

    @VisibleForTesting
    public static ExpirationDateOverflowPolicy policy;

    static {
        String policyAsString = System.getProperty("cassandra.expiration_date_overflow_policy", ExpirationDateOverflowPolicy.REJECT.name());
        try
        {
            policy = ExpirationDateOverflowPolicy.valueOf(policyAsString.toUpperCase());
        }
        catch (RuntimeException e)
        {
            logger.warn("Invalid expiration date overflow policy: {}. Using default: {}", policyAsString, ExpirationDateOverflowPolicy.REJECT.name());
            policy = ExpirationDateOverflowPolicy.REJECT;
        }
    }

    public static final String MAXIMUM_EXPIRATION_DATE_EXCEEDED_WARNING = "Request on table {}.{} with {}ttl of {} seconds exceeds maximum supported expiration " +
                                                                          "date of 2038-01-19T03:14:06+00:00 and will have its expiration capped to that date. " +
                                                                          "In order to avoid this use a lower TTL or upgrade to a version where this limitation " +
                                                                          "is fixed. See CASSANDRA-14092 for more details.";

    public static final String MAXIMUM_EXPIRATION_DATE_EXCEEDED_REJECT_MESSAGE = "Request on table %s.%s with %sttl of %d seconds exceeds maximum supported expiration " +
                                                                                 "date of 2038-01-19T03:14:06+00:00. In order to avoid this use a lower TTL, change " +
                                                                                 "the expiration date overflow policy or upgrade to a version where this limitation " +
                                                                                 "is fixed. See CASSANDRA-14092 for more details.";

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

    public int getTimeToLive(QueryOptions options, CFMetaData metadata) throws InvalidRequestException
    {
        if (timeToLive == null)
        {
            maybeApplyExpirationDateOverflowPolicy(metadata, metadata.getDefaultTimeToLive(), true);
            return metadata.getDefaultTimeToLive();
        }

        ByteBuffer tval = timeToLive.bindAndGet(options);
        if (tval == null)
            throw new InvalidRequestException("Invalid null value of TTL");

        if (tval == ByteBufferUtil.UNSET_BYTE_BUFFER) // treat as unlimited
            return 0;

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

        if (ttl > ExpiringCell.MAX_TTL)
            throw new InvalidRequestException(String.format("ttl is too large. requested (%d) maximum (%d)", ttl, ExpiringCell.MAX_TTL));

        maybeApplyExpirationDateOverflowPolicy(metadata, ttl, false);

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

    public static void maybeApplyExpirationDateOverflowPolicy(CFMetaData metadata, int ttl, boolean isDefaultTTL) throws InvalidRequestException
    {
        if (ttl == 0)
            return;

        // Check for localExpirationTime overflow (CASSANDRA-14092)
        int nowInSecs = (int)(System.currentTimeMillis() / 1000);
        if (ttl + nowInSecs < 0)
        {
            switch (policy)
            {
                case CAP:
                    /**
                     * Capping at this stage is basically not rejecting the request. The actual capping is done
                     * by {@link org.apache.cassandra.db.BufferExpiringCell#computeLocalExpirationTime(int)},
                     * which converts the negative TTL to {@link org.apache.cassandra.db.BufferExpiringCell#MAX_DELETION_TIME}
                     */
                    NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, EXPIRATION_OVERFLOW_WARNING_INTERVAL_MINUTES,
                                     TimeUnit.MINUTES, MAXIMUM_EXPIRATION_DATE_EXCEEDED_WARNING,
                                     metadata.ksName, metadata.cfName, isDefaultTTL? "default " : "", ttl);
                    return;

                default: //REJECT
                    throw new InvalidRequestException(String.format(MAXIMUM_EXPIRATION_DATE_EXCEEDED_REJECT_MESSAGE, metadata.ksName, metadata.cfName,
                                                                    isDefaultTTL? "default " : "", ttl));
            }
        }
    }
}
