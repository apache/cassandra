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

package org.apache.cassandra.db.compaction;

import java.text.DateFormat;
import java.util.Date;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The statistics for time tiered compaction.
 * <p/>
 * Implements serializable to allow structured info to be returned via JMX.
 */
public class TimeTieredCompactionStatistics extends TieredCompactionStatistics
{
    protected static final DateFormat bucketFormatter = DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.SHORT);

    /** The timestamp in this tier */
    private final long timestamp;

    TimeTieredCompactionStatistics(long timestamp,
                                   double hotness,
                                   int numCompactions,
                                   int numCompactionsInProgress,
                                   int numSSTables,
                                   int numCandidateSSTables,
                                   int numCompactingSSTables,
                                   long sizeInBytes,
                                   double readThroughput,
                                   double writeThroughput,
                                   long tot,
                                   long read,
                                   long written)
    {
        super(numCompactions, numCompactionsInProgress, numSSTables, numCandidateSSTables, numCompactingSSTables, sizeInBytes, readThroughput, writeThroughput, hotness, tot, read, written);

        this.timestamp = timestamp;
    }

    /** The timestamp in this tier */
    public long timestamp()
    {
        return timestamp;
    }

    @Override
    @JsonProperty("Bucket")
    protected String tierValue()
    {
        return bucketFormatter.format(new Date(timestamp));
    }
}
