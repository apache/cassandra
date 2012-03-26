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
package org.apache.cassandra.service;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Iterables;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.IdentityQueryFilter;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.net.IAsyncResult;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.FBUtilities;

public class RowRepairResolver extends AbstractRowResolver
{
    protected int maxLiveColumns = 0;
    public List<IAsyncResult> repairResults = Collections.emptyList();

    public RowRepairResolver(String table, ByteBuffer key)
    {
        super(key, table);
    }

    /*
    * This method handles the following scenario:
    *
    * there was a mismatch on the initial read, so we redid the digest requests
    * as full data reads.  In this case we need to compute the most recent version
    * of each column, and send diffs to out-of-date replicas.
    */
    public Row resolve() throws DigestMismatchException, IOException
    {
        if (logger.isDebugEnabled())
            logger.debug("resolving " + replies.size() + " responses");
        long startTime = System.currentTimeMillis();

        ColumnFamily resolved;
        if (replies.size() > 1)
        {
            List<ColumnFamily> versions = new ArrayList<ColumnFamily>(replies.size());
            List<InetAddress> endpoints = new ArrayList<InetAddress>(replies.size());

            for (MessageIn<ReadResponse> message : replies)
            {
                ReadResponse response = message.payload;
                ColumnFamily cf = response.row().cf;
                assert !response.isDigestQuery() : "Received digest response to repair read from " + message.from;
                versions.add(cf);
                endpoints.add(message.from);

                // compute maxLiveColumns to prevent short reads -- see https://issues.apache.org/jira/browse/CASSANDRA-2643
                int liveColumns = cf == null ? 0 : cf.getLiveColumnCount();
                if (liveColumns > maxLiveColumns)
                    maxLiveColumns = liveColumns;
            }

            resolved = resolveSuperset(versions);
            if (logger.isDebugEnabled())
                logger.debug("versions merged");

            // send updates to any replica that was missing part of the full row
            // (resolved can be null even if versions doesn't have all nulls because of the call to removeDeleted in resolveSuperSet)
            if (resolved != null)
                repairResults = scheduleRepairs(resolved, table, key, versions, endpoints);
        }
        else
        {
            resolved = replies.iterator().next().payload.row().cf;
        }

        if (logger.isDebugEnabled())
            logger.debug("resolve: " + (System.currentTimeMillis() - startTime) + " ms.");

        return new Row(key, resolved);
    }

    /**
     * For each row version, compare with resolved (the superset of all row versions);
     * if it is missing anything, send a mutation to the endpoint it come from.
     */
    public static List<IAsyncResult> scheduleRepairs(ColumnFamily resolved, String table, DecoratedKey key, List<ColumnFamily> versions, List<InetAddress> endpoints)
    {
        List<IAsyncResult> results = new ArrayList<IAsyncResult>(versions.size());

        for (int i = 0; i < versions.size(); i++)
        {
            ColumnFamily diffCf = ColumnFamily.diff(versions.get(i), resolved);
            if (diffCf == null) // no repair needs to happen
                continue;

            // create and send the row mutation message based on the diff
            RowMutation rowMutation = new RowMutation(table, key.key);
            rowMutation.add(diffCf);
            MessageOut repairMessage;
            // use a separate verb here because we don't want these to be get the white glove hint-
            // on-timeout behavior that a "real" mutation gets
            repairMessage = rowMutation.createMessage(MessagingService.Verb.READ_REPAIR);
            results.add(MessagingService.instance().sendRR(repairMessage, endpoints.get(i)));
        }

        return results;
    }

    static ColumnFamily resolveSuperset(Iterable<ColumnFamily> versions)
    {
        assert Iterables.size(versions) > 0;

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
        List<CloseableIterator<IColumn>> iters = new ArrayList<CloseableIterator<IColumn>>();
        for (ColumnFamily version : versions)
        {
            if (version == null)
                continue;
            iters.add(FBUtilities.closeableIterator(version.iterator()));
        }
        filter.collateColumns(resolved, iters, Integer.MIN_VALUE);
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

    public int getMaxLiveColumns()
    {
        return maxLiveColumns;
    }
}
