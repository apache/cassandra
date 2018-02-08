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

package org.apache.cassandra.db;

import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.MessageFormatter;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.Attributes;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.utils.NoSpamLogger;

public class ExpirationDateOverflowHandling
{
    private static final Logger logger = LoggerFactory.getLogger(Attributes.class);

    private static final int EXPIRATION_OVERFLOW_WARNING_INTERVAL_MINUTES = Integer.getInteger("cassandra.expiration_overflow_warning_interval_minutes", 5);

    public enum ExpirationDateOverflowPolicy
    {
        REJECT, CAP_NOWARN, CAP
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

    public static void maybeApplyExpirationDateOverflowPolicy(CFMetaData metadata, int ttl, boolean isDefaultTTL) throws InvalidRequestException
    {
        if (ttl == BufferCell.NO_TTL)
            return;

        // Check for localExpirationTime overflow (CASSANDRA-14092)
        int nowInSecs = (int)(System.currentTimeMillis() / 1000);
        if (ttl + nowInSecs < 0)
        {
            switch (policy)
            {
                case CAP:
                    ClientWarn.instance.warn(MessageFormatter.arrayFormat(MAXIMUM_EXPIRATION_DATE_EXCEEDED_WARNING, new Object[] { metadata.ksName,
                                                                                                                                   metadata.cfName,
                                                                                                                                   isDefaultTTL? "default " : "", ttl })
                                                             .getMessage());
                case CAP_NOWARN:
                    /**
                     * Capping at this stage is basically not rejecting the request. The actual capping is done
                     * by {@link #computeLocalExpirationTime(int, int)}, which converts the negative TTL
                     * to {@link org.apache.cassandra.db.BufferExpiringCell#MAX_DELETION_TIME}
                     */
                    NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, EXPIRATION_OVERFLOW_WARNING_INTERVAL_MINUTES, TimeUnit.MINUTES, MAXIMUM_EXPIRATION_DATE_EXCEEDED_WARNING,
                                     metadata.ksName, metadata.cfName, isDefaultTTL? "default " : "", ttl);
                    return;

                default:
                    throw new InvalidRequestException(String.format(MAXIMUM_EXPIRATION_DATE_EXCEEDED_REJECT_MESSAGE, metadata.ksName, metadata.cfName,
                                                                    isDefaultTTL? "default " : "", ttl));
            }
        }
    }

    /**
     * This method computes the {@link Cell#localDeletionTime()}, maybe capping to the maximum representable value
     * which is {@link Cell#MAX_DELETION_TIME}.
     *
     * Please note that the {@link ExpirationDateOverflowHandling.ExpirationDateOverflowPolicy} is applied
     * during {@link ExpirationDateOverflowHandling#maybeApplyExpirationDateOverflowPolicy(CFMetaData, int, boolean)},
     * so if the request was not denied it means its expiration date should be capped.
     *
     * See CASSANDRA-14092
     */
    public static int computeLocalExpirationTime(int nowInSec, int timeToLive)
    {
        int localExpirationTime = nowInSec + timeToLive;
        return localExpirationTime >= 0? localExpirationTime : Cell.MAX_DELETION_TIME;
    }
}
