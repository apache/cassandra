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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.rows.Unfiltered;

import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.NoSpamLogger;


/**
 * Handles unfiltered validation - if configured, it checks if the provided unfiltered has
 * invalid deletions (if the local deletion time is negative or if the ttl is negative) and
 * then either logs or throws an exception if so.
 */
public class UnfilteredValidation
{
    private static final Logger logger = LoggerFactory.getLogger(UnfilteredValidation.class);
    private static final NoSpamLogger nospam1m = NoSpamLogger.getLogger(logger, 1, TimeUnit.MINUTES);

    public static void maybeValidateUnfiltered(Unfiltered unfiltered, TableMetadata metadata, DecoratedKey key, SSTableReader sstable)
    {
        Config.CorruptedTombstoneStrategy strat = DatabaseDescriptor.getCorruptedTombstoneStrategy();
        if (strat != Config.CorruptedTombstoneStrategy.disabled && unfiltered != null && !unfiltered.isEmpty())
        {
            boolean hasInvalidDeletions = false;
            try
            {
                hasInvalidDeletions = unfiltered.hasInvalidDeletions();
            }
            catch (Throwable t) // make sure no unknown exceptions fail the read/compaction
            {
                nospam1m.error("Could not check if Unfiltered in {} had any invalid deletions", sstable, t);
            }

            if (hasInvalidDeletions)
            {
                String content;
                try
                {
                    content = unfiltered.toString(metadata, true);
                }
                catch (Throwable t)
                {
                    content = "Could not get string representation: " + t.getMessage();
                }
                handleInvalid(metadata, key, sstable, content);
            }
        }
    }

    public static void handleInvalid(TableMetadata metadata, DecoratedKey key, SSTableReader sstable, String invalidContent)
    {
        Config.CorruptedTombstoneStrategy strat = DatabaseDescriptor.getCorruptedTombstoneStrategy();
        String keyString;
        try
        {
            keyString = metadata.partitionKeyType.getString(key.getKey());
        }
        catch (Throwable t)
        {
            keyString = "[corrupt token="+key.getToken()+"]";
        }

        if (strat == Config.CorruptedTombstoneStrategy.exception)
        {
            String msg = String.format("Key %s in %s.%s is invalid in %s: %s",
                                       keyString,
                                       metadata.keyspace,
                                       metadata.name,
                                       sstable,
                                       invalidContent);
            // we mark suspect to make sure this sstable is not included in future compactions - it would just keep
            // throwing exceptions
            sstable.markSuspect();
            throw new CorruptSSTableException(new MarshalException(msg), sstable.getFilename());
        }
        else if (strat == Config.CorruptedTombstoneStrategy.warn)
        {
            String msgTemplate = String.format("Key {} in %s.%s is invalid in %s: {}",
                                               metadata.keyspace,
                                               metadata.name,
                                               sstable);
            nospam1m.warn(msgTemplate, keyString, invalidContent);
        }
    }
}
