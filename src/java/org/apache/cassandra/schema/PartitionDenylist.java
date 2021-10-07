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

package org.apache.cassandra.schema;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.reads.range.RangeCommands;
import org.apache.cassandra.utils.Hex;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.cql3.QueryProcessor.process;

/**
 * PartitionDenylist uses the system_distributed.partition_denylist table to maintain a list of denylisted partition keys
 * for each keyspace/table.
 *
 * Keys can be entered manually into the partition_denylist table or via the JMX operation StorageProxyMBean.denylistKey
 *
 * The denylist is stored as one CQL partition per table, and the denylisted keys are column names in that partition. The denylisted
 * keys for each table are cached in memory, and reloaded from the partition_denylist table every 24 hours (default) or when the
 * StorageProxyMBean.loadPartitionDenylist is called via JMX.
 *
 * Concurrency of the cache is provided by the concurrency semantics of the guava LoadingCache. All values (DenylistEntry) are
 * immutable collections of keys/tokens which are replaced in whole when the cache refreshes from disk.
 */
public class PartitionDenylist
{
    private static final Logger logger = LoggerFactory.getLogger(PartitionDenylist.class);
    public static final String PARTITION_DENYLIST_TABLE = "partition_denylist";

    private final ExecutorService executor = new ThreadPoolExecutor(2, 2, Long.MAX_VALUE, TimeUnit.SECONDS, new LinkedBlockingQueue<>());

    /** We effectively don't use our initial empty cache to denylist until the {@link #load()} call which will replace it */
    private volatile LoadingCache<TableId, DenylistEntry> denylist = buildEmptyCache();

    /**
     * Denylist entry is never mutated once constructed, only replaced with a new entry when the cache is refreshed
     */
    private static class DenylistEntry
    {
        public final ImmutableSet<ByteBuffer> keys;
        public final ImmutableSortedSet<Token> tokens;

        public DenylistEntry()
        {
            keys = ImmutableSet.of();
            tokens = ImmutableSortedSet.of();
        }

        public DenylistEntry(final ImmutableSet<ByteBuffer> keys, final ImmutableSortedSet<Token> tokens)
        {
            this.keys = keys;
            this.tokens = tokens;
        }
    }

    // synchronized on this
    private int loadAttempts = 0;
    private int loadSuccesses = 0;

    public synchronized int getLoadAttempts()
    {
        return loadAttempts;
    }
    public synchronized int getLoadSuccesses()
    {
        return loadSuccesses;
    }

    /**
     * Performs initial load of the partition denylist.  Should be called at startup and only loads if the operation
     * is expected to succeed.  If it is not possible to load at call time, a timer is set to retry.
     */
    public void initialLoad()
    {
        if (!DatabaseDescriptor.getEnablePartitionDenylist())
            return;

        synchronized (this)
        {
            loadAttempts++;
        }

        // Check if there are sufficient nodes to attempt reading all the denylist partitions before issuing the query.
        // The pre-check prevents definite range-slice unavailables being marked and triggering an alert.  Nodes may still change
        // state between the check and the query, but it should significantly reduce the alert volume.
        String retryReason = "Insufficient nodes";
        try
        {
            if (readAllHasSufficientNodes() && load())
            {
                return;
            }
        }
        catch (Throwable tr)
        {
            logger.error("Failed to load partition denylist", tr);
            retryReason = "Exception";
        }

        // This path will also be taken on other failures other than UnavailableException,
        // but seems like a good idea to retry anyway.
        int retryInSeconds = DatabaseDescriptor.getDenylistInitialLoadRetrySeconds();
        logger.info(retryReason + " while loading partition denylist cache.  Scheduled retry in {} seconds.",
                    retryInSeconds);
        ScheduledExecutors.optionalTasks.schedule(this::initialLoad, retryInSeconds, TimeUnit.SECONDS);
    }

    private boolean readAllHasSufficientNodes()
    {
        return RangeCommands.sufficientLiveNodesForSelectStar(SystemDistributedKeyspace.PartitionDenylistTable,
                                                              DatabaseDescriptor.getDenylistConsistencyLevel());
    }

    /** Helper method as we need to both build cache on initial init but also on reload of cache contents and params */
    private LoadingCache<TableId, DenylistEntry> buildEmptyCache()
    {
        return Caffeine.newBuilder()
                       .refreshAfterWrite(DatabaseDescriptor.getDenylistRefreshSeconds(), TimeUnit.SECONDS)
                       .executor(executor)
                       .build(new CacheLoader<TableId, DenylistEntry>()
                       {
                           @Override
                           public DenylistEntry load(final TableId tid) throws Exception
                           {
                               return getDenylistForTableFromCQL(tid);
                           }

                           @Override
                           public DenylistEntry reload(final TableId tid, final DenylistEntry oldValue)
                           {
                               /* We accept DenylistEntry data in the following precedence:
                                    1) new data pulled from CQL if we got it correctly,
                                    2) the old value from the cache if there was some error querying,
                                    3) an empty record otherwise.
                               */
                               final DenylistEntry newEntry = getDenylistForTableFromCQL(tid);
                               if (newEntry != null)
                                   return newEntry;
                               if (oldValue != null)
                                   return oldValue;
                               return new DenylistEntry();
                           }
                       });
    }

    /**
     * We need to fully rebuild a new cache to accommodate deleting items from the denylist and potentially shrinking
     * the max allowable size in the list. We do not serve queries out of this denylist until it is populated
     * so as not to introduce a window of having a partially filled cache allow denylisted entries.
     */
    public boolean load()
    {
        final long start = System.currentTimeMillis();

        final Map<TableId, DenylistEntry> allDenylists = getDenylistForAllTablesFromCQL();
        if (allDenylists == null)
            return false;

        // On initial load we have the slight overhead of GC'ing our initial empty cache
        LoadingCache<TableId, DenylistEntry> newDenylist = buildEmptyCache();
        newDenylist.putAll(allDenylists);

        synchronized (this)
        {
            loadSuccesses++;
        }
        denylist = newDenylist;
        logger.info("Loaded partition denylist cache in {}ms", System.currentTimeMillis() - start);
        return true;
    }

    /**
     * We expect the caller to confirm that we are working with a valid keyspace and table. Further, we expect the usage
     * pattern of this to be one-off key by key, not in a bulk process, so we relead the entire table's deny list entry
     * on an addition or removal.
     */
    public boolean addKeyToDenylist(final String keyspace, final String table, final ByteBuffer key)
    {
        if (!isPermitted(keyspace))
            return false;

        final byte[] keyBytes = new byte[key.remaining()];
        key.slice().get(keyBytes);

        final String insert = String.format("INSERT INTO system_distributed.partition_denylist (ks_name, cf_name, key) VALUES ('%s', '%s', 0x%s)",
                                            keyspace, table, Hex.bytesToHex(keyBytes));

        try
        {
            process(insert, DatabaseDescriptor.getDenylistConsistencyLevel());
            return true;
        }
        catch (final RequestExecutionException e)
        {
            logger.error("Failed to denylist key [{}] in {}/{}", Hex.bytesToHex(keyBytes), keyspace, table, e);
        }
        return false;
    }

    /**
     * We expect the caller to confirm that we are working with a valid keyspace and table.
     */
    public boolean removeKeyFromDenylist(final String keyspace, final String table, final ByteBuffer key)
    {
        final byte[] keyBytes = new byte[key.remaining()];
        key.slice().get(keyBytes);

        final String delete = String.format("DELETE FROM system_distributed.partition_denylist " +
                                            "WHERE ks_name = '%s' " +
                                            "AND cf_name = '%s' " +
                                            "AND key = 0x%s",
                                            keyspace, table, Hex.bytesToHex(keyBytes));

        try
        {
            process(delete, DatabaseDescriptor.getDenylistConsistencyLevel());
            return true;
        }
        catch (final RequestExecutionException e)
        {
            logger.error("Failed to remove key from denylist: [{}] in {}/{}", Hex.bytesToHex(keyBytes), keyspace, table, e);
        }
        return false;
    }

    /**
     * We disallow denylisting partitions in certain critical keyspaces to prevent users from making their clusters
     * inoperable.
     */
    private boolean isPermitted(final String keyspace)
    {
        return !SchemaConstants.DISTRIBUTED_KEYSPACE_NAME.equals(keyspace) &&
               !SchemaConstants.SYSTEM_KEYSPACE_NAME.equals(keyspace) &&
               !SchemaConstants.TRACE_KEYSPACE_NAME.equals(keyspace) &&
               !SchemaConstants.AUTH_KEYSPACE_NAME.equals(keyspace);
    }

    public boolean isKeyPermitted(final String keyspace, final String table, final ByteBuffer key)
    {
        return isKeyPermitted(getTableId(keyspace, table), key);
    }

    public boolean isKeyPermitted(final TableId tid, final ByteBuffer key)
    {
        final TableMetadata tmd = Schema.instance.getTableMetadata(tid);
        if (!DatabaseDescriptor.getEnablePartitionDenylist() || tid == null || !isPermitted(tmd.keyspace))
            return true;

        try
        {
            return !denylist.get(tid).keys.contains(key);
        }
        catch (final Exception e)
        {
            // In the event of an error accessing or populating the cache, assume it's not denylisted
            logAccessFailure(tmd, e);
            return true;
        }
    }

    private void logAccessFailure(final TableMetadata tmd, Throwable e)
    {
        if (tmd == null)
            logger.debug("Failed to access partition denylist cache for unknown table id {}", tmd.id, e);
        else
            logger.debug("Failed to access partition denylist cache for {}/{}", tmd.keyspace, tmd.name, e);
    }

    /**
     * @return number of denylisted keys in range
     */
    public int getDeniedKeysInRange(final String keyspace, final String table, final AbstractBounds<PartitionPosition> range)
    {
        return getDeniedKeysInRange(getTableId(keyspace, table), range);
    }

    /**
     * @return number of denylisted keys in range
     */
    public int getDeniedKeysInRange(final TableId tid, final AbstractBounds<PartitionPosition> range)
    {
        final TableMetadata tmd = Schema.instance.getTableMetadata(tid);
        if (!DatabaseDescriptor.getEnablePartitionDenylist() || tid == null || !isPermitted(tmd.keyspace))
            return 0;

        try
        {
            final DenylistEntry denylistEntry = denylist.get(tid);
            if (denylistEntry.tokens.size() == 0)
                return 0;
            final Token startToken = range.left.getToken();
            final Token endToken = range.right.getToken();

            // Normal case
            if (startToken.compareTo(endToken) <= 0 || endToken.isMinimum())
            {
                NavigableSet<Token> subSet = denylistEntry.tokens.tailSet(startToken, PartitionPosition.Kind.MIN_BOUND.equals(range.left.kind()));
                if (!endToken.isMinimum())
                    subSet = subSet.headSet(endToken, PartitionPosition.Kind.MAX_BOUND.equals(range.right.kind()));
                return subSet.size();
            }

            // Wrap around case
            return denylistEntry.tokens.tailSet(startToken, PartitionPosition.Kind.MIN_BOUND.equals(range.left.kind())).size()
                   + denylistEntry.tokens.headSet(endToken, PartitionPosition.Kind.MAX_BOUND.equals(range.right.kind())).size();
        }
        catch (final Exception e)
        {
            logAccessFailure(tmd, e);
            return 0;
        }
    }

    /**
     * Attempts to reload the DenylistEntry data from CQL for the given TableId
     * @return null if we do not find the data; allows cache reloader to preserve old value
     */
    private DenylistEntry getDenylistForTableFromCQL(final TableId tid)
    {
        final TableMetadata tmd = Schema.instance.getTableMetadata(tid);
        if (tmd == null)
            return null;

        // We pull max keys + 1 in order to check below whether we've surpassed the allowable limit or not
        final String readDenylist = String.format("SELECT * FROM %s.%s WHERE ks_name='%s' AND cf_name='%s' LIMIT %d", SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, PARTITION_DENYLIST_TABLE,
                                                  tmd.keyspace, tmd.name, DatabaseDescriptor.getDenylistMaxKeysPerTable() + 1);

        try
        {
            final UntypedResultSet results = process(readDenylist, DatabaseDescriptor.getDenylistConsistencyLevel());

            // If there's no data in CQL we want to return an empty DenylistEntry so we don't continue using the old value in the cache
            if (results == null || results.isEmpty())
                return new DenylistEntry();

            if (results.size() > DatabaseDescriptor.getDenylistMaxKeysPerTable())
                logger.error("Partition denylist for {}/{} has exceeded the maximum allowable size ({}). Remaining keys were ignored; please reduce the number of keys denied or increase the size of your cache to avoid inconsistency in denied partitions across nodes.", tmd.keyspace, tmd.name, DatabaseDescriptor.getDenylistMaxKeysPerTable());

            final Set<ByteBuffer> keys = new HashSet<>();
            final NavigableSet<Token> tokens = new TreeSet<>();
            for (final UntypedResultSet.Row row : results)
            {
                final ByteBuffer key = row.getBlob("key");
                keys.add(key);
                tokens.add(StorageService.instance.getTokenMetadata().partitioner.getToken(key));
            }
            return new DenylistEntry(ImmutableSet.copyOf(keys), ImmutableSortedSet.copyOf(tokens));
        }
        catch (final RequestExecutionException e)
        {
            logger.error("Error reading partition_denylist table for {}/{}. Returning empty list.", tmd.keyspace, tmd.name, e);
            return null;
        }
    }

    /**
     * We check in this method whether either a table or the sum of all denylist keys surpass our allowable limits and
     * warn to the user if we're above our threshold.
     */
    private Map<TableId, DenylistEntry> getDenylistForAllTablesFromCQL()
    {
        /* NEW LOGIC:
            1) Query out all ks_name, cf_name combinations from the table
            2) Iterate through those combos
            3) For each one:
                a) getDenylistForTableFromCQL
                b) Respect and increment the count
                c)
         */



        // We pull max keys + 1 in order to check below whether we've surpassed the allowable limit or not
        final String readDenylist = String.format("SELECT * FROM %s.%s LIMIT %d", SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, PARTITION_DENYLIST_TABLE, DatabaseDescriptor.getDenylistMaxKeysTotal() + 1);
        try
        {
            final UntypedResultSet results = process(readDenylist, DatabaseDescriptor.getDenylistConsistencyLevel());
            if (results == null || results.isEmpty())
                return new HashMap<>();

            if (results.size() > DatabaseDescriptor.getDenylistMaxKeysTotal())
                logger.error("Partition denylist has exceeded the maximum allowable total size ({}). Remaining keys were ignored; please reduce the number of keys denied or increase the size of your cache to avoid inconsistency in denied partitions across nodes.", DatabaseDescriptor.getDenylistMaxKeysTotal());

            final Map<TableId, Pair<Set<ByteBuffer>, NavigableSet<Token>>> allDenylists = new HashMap<>();
            for (final UntypedResultSet.Row row : results)
            {
                final TableId tid = getTableId(row.getString("ks_name"), row.getString("cf_name"));
                if (tid == null)
                    continue;

                Pair<Set<ByteBuffer>, NavigableSet<Token>> pair =
                    allDenylists.computeIfAbsent(tid, k -> Pair.create(new HashSet<>(), new TreeSet<>()));

                final ByteBuffer key = row.getBlob("key");
                pair.left.add(key);
                pair.right.add(StorageService.instance.getTokenMetadata().partitioner.getToken(key));
            }

            final Map<TableId, DenylistEntry> ret = new HashMap<>();
            for (final TableId tid : allDenylists.keySet())
            {
                final Pair<Set<ByteBuffer>, NavigableSet<Token>> entries = allDenylists.get(tid);
                ret.put(tid, new DenylistEntry(ImmutableSet.copyOf(entries.left), ImmutableSortedSet.copyOf(entries.right)));
            }
            return ret;
        }
        catch (final RequestExecutionException e)
        {
            logger.error("Error reading partition_denylist table on startup", e);
            return null;
        }
    }

    private TableId getTableId(final String keyspace, final String table)
    {
        TableMetadata tmd = Schema.instance.getTableMetadata(keyspace, table);
        return tmd == null ? null : tmd.id;
    }
}