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

package org.apache.cassandra.db.monitoring;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.NoSpamLogger;

public class TierMismatch extends BadQueryTypes
{
    protected static final Logger logger = LoggerFactory.getLogger(TierMismatch.class);

    private final String message;

    public TierMismatch(int serviceTier, String serviceName, int cassandraTier)
    {
        super("", "");
        message = String.format("Service Tier = %d (%s) is lower than Cassandra Tier = %d. " +
                               "More critical services should not connect to less critical Cassandra instances",
                               serviceTier, serviceName, cassandraTier);
    }

    @Override
    public String toString() { return message; }

    @Override
    public String getKey()
    {
        return "";
    }

    @Override
    public String getDetails()
    {
        return message;
    }

    static void checkForTierMismatch(String serviceTierStr, String serviceName)
    {
        try
        {
            int serviceTier = Integer.parseInt(serviceTierStr);
            int cassandraTier = CassandraRelevantProperties.DB_TIER.getInt();
            if (serviceTier >= 0 && cassandraTier >= 0 && serviceTier < cassandraTier)
            {
                BadQuery.report(BadQuery.BadQueryCategory.TIER_MISMATCH, new TierMismatch(serviceTier, serviceName, cassandraTier));
            }
        } catch (ConfigurationException e) {
            NoSpamLogger.log(logger, NoSpamLogger.Level.ERROR, 1, TimeUnit.HOURS,
                             "Cassandra tier is ill-formatted: {}", e.getMessage());
        } catch (NumberFormatException e) {
            NoSpamLogger.log(logger, NoSpamLogger.Level.ERROR, 1, TimeUnit.HOURS,
                             "Tier Mismatch check error: {}", e.getMessage());
        }
    }
}
