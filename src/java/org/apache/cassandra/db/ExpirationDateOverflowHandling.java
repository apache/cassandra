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

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.utils.NoSpamLogger;

import static org.apache.cassandra.config.CassandraRelevantProperties.EXPIRATION_DATE_OVERFLOW_POLICY;
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;

public class ExpirationDateOverflowHandling
{
    private static final Logger logger = LoggerFactory.getLogger(ExpirationDateOverflowHandling.class);

    private static final int EXPIRATION_OVERFLOW_WARNING_INTERVAL_MINUTES = CassandraRelevantProperties.EXPIRATION_OVERFLOW_WARNING_INTERVAL_MINUTES.getInt();

    public enum ExpirationDateOverflowPolicy
    {
        REJECT, CAP_NOWARN, CAP
    }

    @VisibleForTesting
    public static ExpirationDateOverflowPolicy policy;

    static {
        try
        {
            policy = EXPIRATION_DATE_OVERFLOW_POLICY.getEnum(ExpirationDateOverflowPolicy.REJECT);
        }
        catch (RuntimeException e)
        {
            logger.warn("Invalid expiration date overflow policy. Using default: {}", ExpirationDateOverflowPolicy.REJECT.name());
            policy = ExpirationDateOverflowPolicy.REJECT;
        }
    }

    public static final String MAXIMUM_EXPIRATION_DATE_EXCEEDED_WARNING = "Request on table {}.{} with {}ttl of {} seconds exceeds maximum supported expiration " +
                                                                          "date of {} and will have its expiration capped to that date. " +
                                                                          "In order to avoid this use a lower TTL or upgrade to a version where this limitation " +
                                                                          "is fixed. See CASSANDRA-14092 and CASSANDRA-14227 for more details.";

    public static final String MAXIMUM_EXPIRATION_DATE_EXCEEDED_REJECT_MESSAGE = "Request on table %s.%s with %sttl of %d seconds exceeds maximum supported expiration " +
                                                                                 "date of %s. In order to avoid this use a lower TTL, change " +
                                                                                 "the expiration date overflow policy or upgrade to a version where this limitation " +
                                                                                 "is fixed. See CASSANDRA-14092 and CASSANDRA-14227 for more details.";

    public static void maybeApplyExpirationDateOverflowPolicy(TableMetadata metadata, int ttl, boolean isDefaultTTL) throws InvalidRequestException
    {
        if (ttl == BufferCell.NO_TTL)
            return;

        // Check for localExpirationTime overflow (CASSANDRA-14092) to apply a policy if needed
        long nowInSecs = currentTimeMillis() / 1000;
        if (((long) ttl + nowInSecs) > Cell.getVersionedMaxDeletiontionTime())
        {
            switch (policy)
            {
                case CAP:
                    ClientWarn.instance.warn(MessageFormatter.arrayFormat(MAXIMUM_EXPIRATION_DATE_EXCEEDED_WARNING, new Object[] { metadata.keyspace,
                                                                                                                                   metadata.name,
                                                                                                                                   isDefaultTTL? "default " : "",
                                                                                                                                   ttl,
                                                                                                                                   getMaxExpirationDateTS()})
                                                             .getMessage());
                case CAP_NOWARN:
                    /**
                     * Capping at this stage is basically not rejecting the request. The actual capping is done
                     * by {@link #computeLocalExpirationTime(long, int)}, which converts the negative TTL
                     * to {@link org.apache.cassandra.db.BufferExpiringCell#MAX_DELETION_TIME}
                     */
                    NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, EXPIRATION_OVERFLOW_WARNING_INTERVAL_MINUTES, TimeUnit.MINUTES, MAXIMUM_EXPIRATION_DATE_EXCEEDED_WARNING,
                                     metadata.keyspace, metadata.name, isDefaultTTL? "default " : "", ttl, getMaxExpirationDateTS());
                    return;

                default:
                    throw new InvalidRequestException(String.format(MAXIMUM_EXPIRATION_DATE_EXCEEDED_REJECT_MESSAGE, metadata.keyspace, metadata.name,
                                                                    isDefaultTTL? "default " : "", ttl, getMaxExpirationDateTS()));
            }
        }
    }

    /**
     * This method computes the {@link Cell#localDeletionTime()}, maybe capping to the maximum representable value
     * which is {@link Cell#MAX_DELETION_TIME}.
     *
     * Please note that the {@link ExpirationDateOverflowHandling.ExpirationDateOverflowPolicy} is applied
     * during {@link ExpirationDateOverflowHandling#maybeApplyExpirationDateOverflowPolicy(org.apache.cassandra.schema.TableMetadata, int, boolean)},
     * so if the request was not denied it means its expiration date should be capped.
     *
     * See CASSANDRA-14092
     */
    public static long computeLocalExpirationTime(long nowInSec, int timeToLive)
    {

        long localExpirationTime = (long) (nowInSec + timeToLive);
        long cellMaxDeletionTime = Cell.getVersionedMaxDeletiontionTime();
        return localExpirationTime <= cellMaxDeletionTime ? localExpirationTime : cellMaxDeletionTime;
    }

    private static String getMaxExpirationDateTS()
    {
        return Cell.getVersionedMaxDeletiontionTime() == Cell.MAX_DELETION_TIME_2038_LEGACY_CAP ? "2038-01-19T03:14:06+00:00"
                                                                                                : "2106-02-07T06:28:13+00:00";
    }
}
