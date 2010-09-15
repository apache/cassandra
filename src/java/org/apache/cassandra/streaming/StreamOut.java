/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.streaming;

import java.net.InetAddress;
import java.util.*;
import java.io.IOException;
import java.io.IOError;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang.StringUtils;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.utils.Pair;

/**
 * This class handles streaming data from one node to another.
 *
 * For StreamingRepair and Unbootstrap
 *  1. The ranges are transferred on a single file basis.
 *  2. Each transfer has the header information for the sstable being transferred.
 *  3. List of the pending files are maintained, as this is the source node.
 *
 * For Stream requests (for bootstrap), the main difference is that we always have to
 * create at least one stream reply, even if the list of files is empty, otherwise the
 * target has no way to know that it can stop waiting for an answer.
 *
 */
public class StreamOut
{
    private static Logger logger = LoggerFactory.getLogger(StreamOut.class);

    /**
     * Split out files for all tables on disk locally for each range and then stream them to the target endpoint.
    */
    public static void transferRanges(InetAddress target, String tableName, Collection<Range> ranges, Runnable callback)
    {
        assert ranges.size() > 0;
        
        // this is so that this target shows up as a destination while anticompaction is happening.
        StreamOutSession session = StreamOutSession.create(tableName, target, callback);

        logger.info("Beginning transfer process to {} for ranges {}", target, StringUtils.join(ranges, ", "));

        try
        {
            Table table = flushSSTable(tableName);
            // send the matching portion of every sstable in the keyspace
            transferSSTables(session, table.getAllSSTables(), ranges);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
        finally
        {
            session.close();
        }
    }

    /**
     * (1) dump all the memtables to disk.
     * (2) determine the minimal file sections we need to send for the given ranges
     * (3) transfer the data.
     */
    private static Table flushSSTable(String tableName) throws IOException
    {
        Table table = Table.open(tableName);
        logger.info("Flushing memtables for {}...", tableName);
        for (Future f : table.flush())
        {
            try
            {
                f.get();
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
            catch (ExecutionException e)
            {
                throw new RuntimeException(e);
            }
        }
        return table;
    }

    /**
     * Split out files for all tables on disk locally for each range and then stream them to the target endpoint.
    */
    public static void transferRangesForRequest(StreamOutSession session, Collection<Range> ranges)
    {
        assert ranges.size() > 0;

        logger.info("Beginning transfer process to {} for ranges {}", session.getHost(), StringUtils.join(ranges, ", "));

        try
        {
            Table table = flushSSTable(session.table);
            // send the matching portion of every sstable in the keyspace
            List<PendingFile> pending = createPendingFiles(table.getAllSSTables(), ranges);
            session.addFilesToStream(pending);
            session.begin();
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    /**
     * Transfers matching portions of a group of sstables from a single table to the target endpoint.
     */
    public static void transferSSTables(StreamOutSession session, Collection<SSTableReader> sstables, Collection<Range> ranges) throws IOException
    {
        List<PendingFile> pending = createPendingFiles(sstables, ranges);

        if (pending.size() > 0)
        {
            session.addFilesToStream(pending);
            session.begin();

            logger.info("Waiting for transfer to {} to complete", session.getHost());
            session.waitForStreamCompletion();
            logger.info("Done with transfer to {}", session.getHost());
        }
    }

    // called prior to sending anything.
    private static List<PendingFile> createPendingFiles(Collection<SSTableReader> sstables, Collection<Range> ranges)
    {
        List<PendingFile> pending = new ArrayList<PendingFile>();
        for (SSTableReader sstable : sstables)
        {
            Descriptor desc = sstable.getDescriptor();
            List<Pair<Long,Long>> sections = sstable.getPositionsForRanges(ranges);
            if (sections.isEmpty())
                continue;
            pending.add(new PendingFile(desc, SSTable.COMPONENT_DATA, sections));
        }
        logger.info("Stream context metadata {}, {} sstables.", pending, sstables.size());
        return pending;
    }
}
