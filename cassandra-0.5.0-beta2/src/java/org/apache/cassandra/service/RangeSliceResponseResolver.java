/**
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

package org.apache.cassandra.service;

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;

import org.apache.log4j.Logger;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.RangeSliceReply;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.net.Message;

/**
 * Turns RangeSliceReply objects into row (string -> CF) maps, resolving
 * to the most recent ColumnFamily and setting up read repairs as necessary.
 */
public class RangeSliceResponseResolver implements IResponseResolver<Map<String, ColumnFamily>>
{
    private static final Logger logger_ = Logger.getLogger(RangeSliceResponseResolver.class);
    private final String table;
    private final Range range;
    private final List<InetAddress> sources;
    private boolean isCompleted;

    public RangeSliceResponseResolver(String table, Range range, List<InetAddress> sources)
    {
        assert sources.size() > 0;
        this.sources = sources;
        this.range = range;
        this.table = table;
    }

    public Map<String, ColumnFamily> resolve(List<Message> responses) throws DigestMismatchException, IOException
    {
        Map<InetAddress, Map<String, ColumnFamily>> replies = new HashMap<InetAddress, Map<String, ColumnFamily>>(responses.size());
        Set<String> allKeys = new HashSet<String>();
        for (Message response : responses)
        {
            RangeSliceReply reply = RangeSliceReply.read(response.getMessageBody());
            isCompleted &= reply.rangeCompletedLocally;
            Map<String, ColumnFamily> rows = new HashMap<String, ColumnFamily>(reply.rows.size());
            for (Row row : reply.rows)
            {
                rows.put(row.key, row.cf);
                allKeys.add(row.key);
            }
            replies.put(response.getFrom(), rows);
        }

        // for each row, compute the combination of all different versions seen, and repair incomplete versions
        // TODO since the rows all arrive in sorted order, we should be able to do this more efficiently w/o all the Map conversion
        Map<String, ColumnFamily> resolvedRows = new HashMap<String, ColumnFamily>(allKeys.size());
        for (String key : allKeys)
        {
            List<ColumnFamily> versions = new ArrayList<ColumnFamily>(sources.size());
            for (InetAddress endpoint : sources)
            {
                versions.add(replies.get(endpoint).get(key));
            }
            ColumnFamily resolved = ReadResponseResolver.resolveSuperset(versions);
            ReadResponseResolver.maybeScheduleRepairs(resolved, table, key, versions, sources);
            resolvedRows.put(key, resolved);
        }
        return resolvedRows;
    }

    public boolean isDataPresent(List<Message> responses)
    {
        return responses.size() >= sources.size();
    }

    /**
     * only valid after resolve has been called (typically via QRH.get)
     */
    public boolean completed()
    {
        return isCompleted;
    }
}