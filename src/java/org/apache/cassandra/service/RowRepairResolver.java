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

import java.io.IOError;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.iterators.CollatingIterator;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.IdentityQueryFilter;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;

public class RowRepairResolver extends AbstractRowResolver
{
    public RowRepairResolver(String table, ByteBuffer key)
    {
        super(key, table);
    }

    /*
    * This method handles the following scenario:
    *
    * there was a mismatch on the initial read (1a or 1b), so we redid the digest requests
    * as full data reads.  In this case we need to compute the most recent version
    * of each column, and send diffs to out-of-date replicas.
    */
    public Row resolve() throws DigestMismatchException, IOException
    {
        if (logger.isDebugEnabled())
            logger.debug("resolving " + replies.size() + " responses");

        long startTime = System.currentTimeMillis();
		List<ColumnFamily> versions = new ArrayList<ColumnFamily>();
		List<InetAddress> endpoints = new ArrayList<InetAddress>();

        // case 1: validate digests against each other; throw immediately on mismatch.
        // also, collects data results into versions/endpoints lists.
        //
        // results are cleared as we process them, to avoid unnecessary duplication of work
        // when resolve() is called a second time for read repair on responses that were not
        // necessary to satisfy ConsistencyLevel.
        for (Map.Entry<Message, ReadResponse> entry : replies.entrySet())
        {
            Message message = entry.getKey();
            ReadResponse response = entry.getValue();
            assert !response.isDigestQuery();
            versions.add(response.row().cf);
            endpoints.add(message.getFrom());
        }

        ColumnFamily resolved;
        if (versions.size() > 1)
        {
            resolved = resolveSuperset(versions);
            if (logger.isDebugEnabled())
                logger.debug("versions merged");
            // resolved can be null even if versions doesn't have all nulls because of the call to removeDeleted in resolveSuperSet
            if (resolved != null)
                maybeScheduleRepairs(resolved, table, key, versions, endpoints);
        }
        else
        {
            resolved = versions.get(0);
        }

        if (logger.isDebugEnabled())
            logger.debug("resolve: " + (System.currentTimeMillis() - startTime) + " ms.");
		return new Row(key, resolved);
	}

    /**
     * For each row version, compare with resolved (the superset of all row versions);
     * if it is missing anything, send a mutation to the endpoint it come from.
     */
    public static void maybeScheduleRepairs(ColumnFamily resolved, String table, DecoratedKey key, List<ColumnFamily> versions, List<InetAddress> endpoints)
    {
        for (int i = 0; i < versions.size(); i++)
        {
            ColumnFamily diffCf = ColumnFamily.diff(versions.get(i), resolved);
            if (diffCf == null) // no repair needs to happen
                continue;

            // create and send the row mutation message based on the diff
            RowMutation rowMutation = new RowMutation(table, key.key);
            rowMutation.add(diffCf);
            Message repairMessage;
            try
            {
                repairMessage = rowMutation.getMessage(Gossiper.instance.getVersion(endpoints.get(i)));
            }
            catch (IOException e)
            {
                throw new IOError(e);
            }
            MessagingService.instance().sendOneWay(repairMessage, endpoints.get(i));
        }
    }

    static ColumnFamily resolveSuperset(List<ColumnFamily> versions)
    {
        assert versions.size() > 0;

        ColumnFamily resolved = null;
        for (ColumnFamily cf : versions)
        {
            if (cf == null)
                continue;

            if (resolved == null)
                resolved = cf.cloneMeShallow();
            else
                resolved.delete(cf);
        }
        if (resolved == null)
            return null;

        // mimic the collectCollatedColumn + removeDeleted path that getColumnFamily takes.
        // this will handle removing columns and subcolumns that are supressed by a row or
        // supercolumn tombstone.
        QueryFilter filter = new QueryFilter(null, new QueryPath(resolved.metadata().cfName), new IdentityQueryFilter());
        CollatingIterator iter = new CollatingIterator(resolved.metadata().comparator.columnComparator);
        for (ColumnFamily version : versions)
        {
            if (version == null)
                continue;
            iter.addIterator(version.getColumnsMap().values().iterator());
        }
        filter.collectCollatedColumns(resolved, iter, Integer.MIN_VALUE);
        return ColumnFamilyStore.removeDeleted(resolved, Integer.MIN_VALUE);
    }

    public Row getData() throws IOException
    {
        throw new UnsupportedOperationException();
    }

    public boolean isDataPresent()
	{
        throw new UnsupportedOperationException();
    }
}
