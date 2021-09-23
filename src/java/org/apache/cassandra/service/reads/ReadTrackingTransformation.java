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

package org.apache.cassandra.service.reads;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.service.QueryInfoTracker;
import org.apache.cassandra.utils.NoSpamLogger;

/**
 * {@code UnfilteredRowIterator} transformation that callbacks {@link QueryInfoTracker.ReadTracker} on
 * each row and partition. The transformation may be extended with other callback methods (like
 * {@code applyToStatic} or {@code applyToDeletion} if necessary.
 *
 * Do not move closing the tracker here (to @{code onClose} method). One read may include more than
 * one row iterator, closing the tracker here may result in multiple close callbacks.
 */
class ReadTrackingTransformation extends Transformation<UnfilteredRowIterator>
{
    private final QueryInfoTracker.ReadTracker readTracker;
    private static final Logger logger = LoggerFactory.getLogger(ReadTrackingTransformation.class);

    public ReadTrackingTransformation(QueryInfoTracker.ReadTracker readTracker)
    {
        this.readTracker = readTracker;
    }

    @Override
    protected UnfilteredRowIterator applyToPartition(UnfilteredRowIterator partition)
    {
        return Transformation.apply(partition, this);
    }

    @Override
    protected Row applyToRow(Row row)
    {
        try
        {
            readTracker.onRow(row);
        }
        catch (Exception exc)
        {
            NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, 60, TimeUnit.SECONDS,
                             "Tracking callback for read rows failed", exc);
        }
        return super.applyToRow(row);
    }

    @Override
    protected DecoratedKey applyToPartitionKey(DecoratedKey key)
    {
        try
        {
            readTracker.onPartition(key);
        }
        catch (Exception exc)
        {
            NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, 60, TimeUnit.SECONDS,
                             "Tracking callback for read partitions failed", exc);
        }
        return super.applyToPartitionKey(key);
    }
}
