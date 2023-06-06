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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.reads.range.RangeCommands;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.NoSpamLogger;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.cql3.QueryProcessor.process;
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;

/**
 * PartitionDenylist uses the system_distributed.partition_denylist table to maintain a list of denylisted partition keys
 * for each keyspace/table.
 *
 * Keys can be entered manually into the partition_denylist table or via the JMX operation StorageProxyMBean.denylistKey
 *
 * The denylist is stored as one CQL partition per table, and the denylisted keys are column names in that partition. The denylisted
 * keys for each table are cached in memory, and reloaded from the partition_denylist table every 10 minutes (default) or when the
 * StorageProxyMBean.loadPartitionDenylist is called via JMX.
 *
 * Concurrency of the cache is provided by the concurrency semantics of the guava LoadingCache. All values (DenylistEntry) are
 * immutable collections of keys/tokens which are replaced in whole when the cache refreshes from disk.
 *
 * The CL for the denylist is used on initial node load as well as on timer instigated cache refreshes. A JMX call by the
 * operator to load the denylist cache will warn on CL unavailability but go through with the denylist load. This is to
 * allow operators flexibility in the face of degraded cluster state and still grant them the ability to mutate the denylist
 * cache and bring it up if there are things they need to block on startup.
 *
 * Notably, in the current design it's possible for a table *cache expiration instigated* reload to end up violating the
 * contract on total denylisted keys allowed in the case where it initially loads with a value less than the DBD
 * allowable max per table limit due to global constraint enforcement on initial load. Our load and reload function
 * simply enforce the *per table* limit without consideration to what that entails at the global key level. While we
 * could track the constrained state and count in DenylistEntry, for now the complexity doesn't seem to justify the
 * protection against that edge case. The enforcement should take place on a user-instigated full reload as well as
 * error messaging about count violations, so this only applies to situations in which someone adds a key and doesn't
 * actively tell the cache to fully reload to take that key into consideration, which one could reasonably expect to be
 * an antipattern.
 */
public class PartitionDenylist
{
    private static final Logger logger = LoggerFactory.getLogger(PartitionDenylist.class);
    private static final NoSpamLogger AVAILABILITY_LOGGER = NoSpamLogger.getLogger(logger, 1, TimeUnit.MINUTES);

    private final ExecutorService executor = executorFactory().pooled("DenylistCache", 2);

    /** We effectively don't use our initial empty cache to denylist until the {@link #load()} call which will replace it */
    private volatile LoadingCache<TableId, DenylistEntry> denylist = buildEmptyCache();

    /** Denylist entry is never mutated once constructed, only replaced with a new entry when the cache is refreshed */
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

    /** synchronized on this */
    private int loadAttempts = 0;

    /** synchronized on this */
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
     * Performs initial load of the partition denylist. Should be called at startup and only loads if the operation
     * is expected to succeed.  If it is not possible to load at call time, a timer is set to retry.
     */
    public void initialLoad()
    {
        if (!DatabaseDescriptor.getPartitionDenylistEnabled())
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
            if (checkDenylistNodeAvailability())
            {
                load();
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
        logger.info("{} while loading partition denylist cache. Scheduled retry in {} seconds.", retryReason, retryInSeconds);
        ScheduledExecutors.optionalTasks.schedule(this::initialLoad, retryInSeconds, TimeUnit.SECONDS);
    }

    private boolean checkDenylistNodeAvailability()
    {
        boolean sufficientNodes = RangeCommands.sufficientLiveNodesForSelectStar(SystemDistributedKeyspace.PartitionDenylistTable,
                                                                                 DatabaseDescriptor.getDenylistConsistencyLevel());
        if (!sufficientNodes)
        {
            AVAILABILITY_LOGGER.warn("Attempting to load denylist and not enough nodes are available for a {} refresh. Reload the denylist when unavailable nodes are recovered to ensure your denylist remains in sync.",
                                     DatabaseDescriptor.getDenylistConsistencyLevel());
        }
        return sufficientNodes;
    }

    /** Helper method as we need to both build cache on initial init but also on reload of cache contents and params */
    private LoadingCache<TableId, DenylistEntry> buildEmptyCache()
    {
        // We rely on details of .refreshAfterWrite to reload this async in the background when it's hit:
        // https://github.com/ben-manes/caffeine/wiki/Refresh
        return Caffeine.newBuilder()
                       .refreshAfterWrite(DatabaseDescriptor.getDenylistRefreshSeconds(), TimeUnit.SECONDS)
                       .executor(executor)
                       .build(new CacheLoader<TableId, DenylistEntry>()
                       {
                           @Override
                           public DenylistEntry load(final TableId tid)
                           {
                               // We load whether or not the CL required count are available as the alternative is an
                               // empty denylist. This allows operators to intervene in the event they need to deny or
                               // undeny a specific partition key around a node recovery.
                               checkDenylistNodeAvailability();
                               return getDenylistForTableFromCQL(tid);
                           }

                           // The synchronous reload method defaults to being wrapped with a supplyAsync in CacheLoader.asyncReload
                           @Override
                           public DenylistEntry reload(final TableId tid, final DenylistEntry oldValue)
                           {
                               // Only process when we can hit the user specified CL for the denylist consistency on a timer prompted reload
                               if (checkDenylistNodeAvailability())
                               {
                                   final DenylistEntry newEntry = getDenylistForTableFromCQL(tid);
                                   if (newEntry != null)
                                       return newEntry;
                               }
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
    public void load()
    {
        final long start = currentTimeMillis();

        final Map<TableId, DenylistEntry> allDenylists = getDenylistForAllTablesFromCQL();

        // On initial load we have the slight overhead of GC'ing our initial empty cache
        LoadingCache<TableId, DenylistEntry> newDenylist = buildEmptyCache();
        newDenylist.putAll(allDenylists);

        synchronized (this)
        {
            loadSuccesses++;
        }
        denylist = newDenylist;
        logger.info("Loaded partition denylist cache in {}ms", currentTimeMillis() - start);
    }

    /**
     * We expect the caller to confirm that we are working with a valid keyspace and table. Further, we expect the usage
     * pattern of this to be one-off key by key, not in a bulk process, so we reload the entire table's deny list entry
     * on an addition or removal.
     */
    public boolean addKeyToDenylist(final String keyspace, final String table, final ByteBuffer key)
    {
        if (!canDenylistKeyspace(keyspace))
            return false;

        final String insert = String.format("INSERT INTO system_distributed.partition_denylist (ks_name, table_name, key) VALUES ('%s', '%s', 0x%s)",
                                            keyspace, table, ByteBufferUtil.bytesToHex(key));

        try
        {
            process(insert, DatabaseDescriptor.getDenylistConsistencyLevel());
            return refreshTableDenylist(keyspace, table);
        }
        catch (final RequestExecutionException e)
        {
            logger.error("Failed to denylist key [{}] in {}/{}", ByteBufferUtil.bytesToHex(key), keyspace, table, e);
        }
        return false;
    }

    /**
     * We expect the caller to confirm that we are working with a valid keyspace and table.
     */
    public boolean removeKeyFromDenylist(final String keyspace, final String table, final ByteBuffer key)
    {
        final String delete = String.format("DELETE FROM system_distributed.partition_denylist " +
                                            "WHERE ks_name = '%s' " +
                                            "AND table_name = '%s' " +
                                            "AND key = 0x%s",
                                            keyspace, table, ByteBufferUtil.bytesToHex(key));

        try
        {
            process(delete, DatabaseDescriptor.getDenylistConsistencyLevel());
            return refreshTableDenylist(keyspace, table);
        }
        catch (final RequestExecutionException e)
        {
            logger.error("Failed to remove key from denylist: [{}] in {}/{}", ByteBufferUtil.bytesToHex(key), keyspace, table, e);
        }
        return false;
    }

    /**
     * We disallow denylisting partitions in certain critical keyspaces to prevent users from making their clusters
     * inoperable.
     */
    private boolean canDenylistKeyspace(final String keyspace)
    {
        return !SchemaConstants.DISTRIBUTED_KEYSPACE_NAME.equals(keyspace) &&
               !SchemaConstants.SYSTEM_KEYSPACE_NAME.equals(keyspace) &&
               !SchemaConstants.TRACE_KEYSPACE_NAME.equals(keyspace) &&
               !SchemaConstants.VIRTUAL_SCHEMA.equals(keyspace) &&
               !SchemaConstants.VIRTUAL_VIEWS.equals(keyspace) &&
               !SchemaConstants.AUTH_KEYSPACE_NAME.equals(keyspace);
    }

    public boolean isKeyPermitted(final String keyspace, final String table, final ByteBuffer key)
    {
        return isKeyPermitted(getTableId(keyspace, table), key);
    }

    public boolean isKeyPermitted(final TableId tid, final ByteBuffer key)
    {
        final TableMetadata tmd = Schema.instance.getTableMetadata(tid);

        // We have a few quick state checks to get out of the way first; this is hot path so we want to do these first if possible.
        if (!DatabaseDescriptor.getPartitionDenylistEnabled() || tid == null || tmd == null || !canDenylistKeyspace(tmd.keyspace))
            return true;

        try
        {
            // If we don't have an entry for this table id, nothing in it is denylisted.
            DenylistEntry entry = denylist.get(tid);
            if (entry == null)
                return true;
            return !entry.keys.contains(key);
        }
        catch (final Exception e)
        {
            // In the event of an error accessing or populating the cache, assume it's not denylisted
            logAccessFailure(tid, e);
            return true;
        }
    }

    private void logAccessFailure(final TableId tid, Throwable e)
    {
        final TableMetadata tmd = Schema.instance.getTableMetadata(tid);
        if (tmd == null)
            logger.debug("Failed to access partition denylist cache for unknown table id {}", tid.toString(), e);
        else
            logger.debug("Failed to access partition denylist cache for {}/{}", tmd.keyspace, tmd.name, e);
    }

    /**
     * @return number of denylisted keys in range
     */
    public int getDeniedKeysInRangeCount(final String keyspace, final String table, final AbstractBounds<PartitionPosition> range)
    {
        return getDeniedKeysInRangeCount(getTableId(keyspace, table), range);
    }

    /**
     * @return number of denylisted keys in range
     */
    public int getDeniedKeysInRangeCount(final TableId tid, final AbstractBounds<PartitionPosition> range)
    {
        final TableMetadata tmd = Schema.instance.getTableMetadata(tid);
        if (!DatabaseDescriptor.getPartitionDenylistEnabled() || tid == null || tmd == null || !canDenylistKeyspace(tmd.keyspace))
            return 0;

        try
        {
            final DenylistEntry denylistEntry = denylist.get(tid);
            if (denylistEntry == null || denylistEntry.tokens.size() == 0)
                return 0;
            final Token startToken = range.left.getToken();
            final Token endToken = range.right.getToken();

            // Normal case
            if (startToken.compareTo(endToken) <= 0 || endToken.isMinimum())
            {
                NavigableSet<Token> subSet = denylistEntry.tokens.tailSet(startToken, PartitionPosition.Kind.MIN_BOUND == range.left.kind());
                if (!endToken.isMinimum())
                    subSet = subSet.headSet(endToken, PartitionPosition.Kind.MAX_BOUND == range.right.kind());
                return subSet.size();
            }

            // Wrap around case
            return denylistEntry.tokens.tailSet(startToken, PartitionPosition.Kind.MIN_BOUND == range.left.kind()).size()
                   + denylistEntry.tokens.headSet(endToken, PartitionPosition.Kind.MAX_BOUND == range.right.kind()).size();
        }
        catch (final Exception e)
        {
            logAccessFailure(tid, e);
            return 0;
        }
    }

    /**
     * Get up to the configured allowable limit per table of denylisted keys
     */
    private DenylistEntry getDenylistForTableFromCQL(final TableId tid)
    {
        return getDenylistForTableFromCQL(tid, DatabaseDescriptor.getDenylistMaxKeysPerTable());
    }

    /**
     * Attempts to reload the DenylistEntry data from CQL for the given TableId and key count.
     * @return empty denylist if we do not or cannot find the data, preserving the old value, otherwise the new value
     */
    private DenylistEntry getDenylistForTableFromCQL(final TableId tid, int limit)
    {
        final TableMetadata tmd = Schema.instance.getTableMetadata(tid);
        if (tmd == null)
            return null;

        // We attempt to query just over our allowable max keys in order to check whether we have configured data beyond that limit and alert the user if so
        final String readDenylist = String.format("SELECT * FROM %s.%s WHERE ks_name='%s' AND table_name='%s' LIMIT %d",
                                                  SchemaConstants.DISTRIBUTED_KEYSPACE_NAME,
                                                  SystemDistributedKeyspace.PARTITION_DENYLIST_TABLE,
                                                  tmd.keyspace,
                                                  tmd.name,
                                                  limit + 1);

        try
        {
            final UntypedResultSet results = process(readDenylist, DatabaseDescriptor.getDenylistConsistencyLevel());

            // If there's no data in CQL we want to return an empty DenylistEntry so we don't continue using the old value in the cache
            if (results == null || results.isEmpty())
                return new DenylistEntry();

            if (results.size() > limit)
            {
                // If our limit is < the standard per table we know we're at a global violation because we've constrained that request limit already.
                boolean globalLimit = limit != DatabaseDescriptor.getDenylistMaxKeysPerTable();
                String violationType = globalLimit ? "global" : "per-table";
                int errorLimit = globalLimit ? DatabaseDescriptor.getDenylistMaxKeysTotal() : limit;
                logger.error("Partition denylist for {}/{} has exceeded the {} allowance of ({}). Remaining keys were ignored; " +
                             "please reduce the total number of keys denied or increase the denylist_max_keys_per_table param in " +
                             "cassandra.yaml to avoid inconsistency in denied partitions across nodes.",
                             tmd.keyspace,
                             tmd.name,
                             violationType,
                             errorLimit);
            }

            final Set<ByteBuffer> keys = new HashSet<>();
            final NavigableSet<Token> tokens = new TreeSet<>();

            int processed = 0;
            for (final UntypedResultSet.Row row : results)
            {
                final ByteBuffer key = row.getBlob("key");
                keys.add(key);
                tokens.add(StorageService.instance.getTokenMetadata().partitioner.getToken(key));

                processed++;
                if (processed >= limit)
                    break;
            }
            return new DenylistEntry(ImmutableSet.copyOf(keys), ImmutableSortedSet.copyOf(tokens));
        }
        catch (final RequestExecutionException e)
        {
            logger.error("Error reading partition_denylist table for {}/{}. Returning empty denylist.", tmd.keyspace, tmd.name, e);
            return new DenylistEntry();
        }
    }

    /**
     * This method relies on {@link #getDenylistForTableFromCQL(TableId, int)} to pull a limited amount of keys
     * on a per-table basis from CQL to load into the cache. We need to navigate both respecting the max cache size limit
     * as well as respecting the per-table limit.
     * @return non-null mapping of TableId to DenylistEntry
     */
    private Map<TableId, DenylistEntry> getDenylistForAllTablesFromCQL()
    {
        // While we warn the user in this case, we continue with the reload anyway.
        checkDenylistNodeAvailability();

        final String allDeniedTables = String.format("SELECT DISTINCT ks_name, table_name FROM %s.%s",
                                                     SchemaConstants.DISTRIBUTED_KEYSPACE_NAME,
                                                     SystemDistributedKeyspace.PARTITION_DENYLIST_TABLE);
        try
        {
            final UntypedResultSet deniedTableResults = process(allDeniedTables, DatabaseDescriptor.getDenylistConsistencyLevel());
            if (deniedTableResults == null || deniedTableResults.isEmpty())
                return Collections.emptyMap();

            int totalProcessed = 0 ;
            final Map<TableId, DenylistEntry> results = new HashMap<>();
            for (final UntypedResultSet.Row row : deniedTableResults)
            {
                final String ks = row.getString("ks_name");
                final String table = row.getString("table_name");
                final TableId tid = getTableId(ks, table);
                if (DatabaseDescriptor.getDenylistMaxKeysTotal() - totalProcessed <= 0)
                {
                    logger.error("Hit limit on allowable denylisted keys in total. Processed {} total entries. Not adding all entries to denylist for {}/{}." +
                                 " Remove denylist entries in system_distributed.{} or increase your denylist_max_keys_total param in cassandra.yaml.",
                                 totalProcessed,
                                 ks,
                                 table,
                                 SystemDistributedKeyspace.PARTITION_DENYLIST_TABLE);
                    results.put(tid, new DenylistEntry());
                }
                else
                {
                    // Determine whether we can get up to table max or we need a subset at edge condition of max overflow.
                    int allowedTableRecords = Math.min(DatabaseDescriptor.getDenylistMaxKeysPerTable(), DatabaseDescriptor.getDenylistMaxKeysTotal() - totalProcessed);
                    DenylistEntry tableDenylist = getDenylistForTableFromCQL(tid, allowedTableRecords);
                    if (tableDenylist != null)
                        totalProcessed += tableDenylist.keys.size();
                    results.put(tid, tableDenylist);
                }
            }
            return results;
        }
        catch (final RequestExecutionException e)
        {
            logger.error("Error reading full partition denylist from "
                         + SchemaConstants.DISTRIBUTED_KEYSPACE_NAME + "." + SystemDistributedKeyspace.PARTITION_DENYLIST_TABLE +
                         ". Partition Denylisting will be compromised. Exception: " + e);
            return Collections.emptyMap();
        }
    }

    private boolean refreshTableDenylist(String keyspace, String table)
    {
        checkDenylistNodeAvailability();
        final TableId tid = getTableId(keyspace, table);
        if (tid == null)
        {
            logger.warn("Got denylist mutation for unknown ks/cf: {}/{}. Skipping refresh.", keyspace, table);
            return false;
        }

        DenylistEntry newEntry = getDenylistForTableFromCQL(tid);
        denylist.put(tid, newEntry);
        return true;
    }

    private TableId getTableId(final String keyspace, final String table)
    {
        TableMetadata tmd = Schema.instance.getTableMetadata(keyspace, table);
        return tmd == null ? null : tmd.id;
    }
}
