package org.apache.cassandra.service.epaxos;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Striped;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

public class KeyStateManager
{
    // prevents multiple threads modifying the dependency manager for a given key. Aquire this after locking an instance
    private final Striped<Lock> locks = Striped.lock(DatabaseDescriptor.getConcurrentWriters() * 1024);
    private final Cache<CfKey, KeyState> cache;
    private final String keyspace;
    private final String table;
    protected final TokenStateManager tokenStateManager;
    private final Scope scope;

    public KeyStateManager(TokenStateManager tokenStateManager, Scope scope)
    {
        this(SystemKeyspace.NAME, SystemKeyspace.EPAXOS_KEY_STATE, tokenStateManager, scope);
    }

    public KeyStateManager(String keyspace, String table, TokenStateManager tokenStateManager, Scope scope)
    {
        this.keyspace = keyspace;
        this.table = table;
        this.tokenStateManager = tokenStateManager;
        this.scope = scope;
        cache = CacheBuilder.newBuilder().expireAfterAccess(5, TimeUnit.MINUTES).maximumSize(1000).build();
    }

    public Scope getScope()
    {
        return scope;
    }

    public Lock getCfKeyLock(CfKey cfKey)
    {
        return locks.get(cfKey);
    }

    protected Iterator<CfKey> getCfKeyIterator(TokenState tokenState)
    {
        return getCfKeyIterator(tokenState, 10000);
    }

    /**
     * Returns an iterator of CfKeys of all keys owned by the given tokenState
     */
    public Iterator<CfKey> getCfKeyIterator(final TokenState tokenState, final int limit)
    {
        return getCfKeyIterator(tokenStateManager.rangeFor(tokenState), tokenState.getCfId(), limit);
    }

    /**
     * Returns an iterator of CfKeys of all keys owned by the given token range
     */
    public Iterator<CfKey> getCfKeyIterator(Range<Token> range, UUID cfId, int limit)
    {
        Function<UntypedResultSet.Row, CfKey> f = new Function<UntypedResultSet.Row, CfKey>()
        {
            public CfKey apply(UntypedResultSet.Row row)
            {
                return new CfKey(row.getBlob("row_key"), row.getUUID("cf_id"));
            }
        };
        Iterable<UntypedResultSet.Row> iterable;
        iterable = new KeyTableIterable(keyspace, table, range, false, limit);
        iterable = Iterables.filter(iterable, new KeyTableIterable.CfIdPredicate(cfId));
        iterable = Iterables.filter(iterable, new KeyTableIterable.ScopePredicate(scope));
        return Iterables.transform(iterable, f).iterator();
    }

    public Pair<Set<UUID>, Range<Token>> getCurrentDependencies(Instance instance)
    {
        if (instance.getType() == Instance.Type.QUERY)
        {
            SerializedRequest request = ((QueryInstance) instance).getQuery();
            CfKey cfKey = request.getCfKey();
            Set<UUID> deps = getCurrentDependencies(instance, cfKey);
            deps.remove(instance.getId());
            return Pair.create(deps, null);
        }
        else if (instance.getType() == Instance.Type.EPOCH || instance.getType() == Instance.Type.TOKEN)
        {
            Set<UUID> deps;
            Range<Token> range;

            TokenState tokenState = tokenStateManager.get(instance);
            tokenState.lock.readLock().lock();
            try
            {
                deps = new HashSet<>(tokenStateManager.getCurrentDependencies((AbstractTokenInstance) instance));

                // create a range using the left token as dictated by the current managed token ring for this cf, and the
                // instance token as the right token. This prevents token instances from having dependencies on instances
                // that some of it's  replicas don't replicate. If a node misses this split, it will find out about it the
                // next time it touches one of the keys owned by the new token
                range = tokenState.getRange();
            }
            finally
            {
                tokenState.lock.readLock().unlock();
            }

            Iterator<CfKey> cfKeyIterator = getCfKeyIterator(range, tokenState.getCfId(), 10000);
            while (cfKeyIterator.hasNext())
            {
                CfKey cfKey = cfKeyIterator.next();
                deps.addAll(getCurrentDependencies(instance, cfKey));
            }

            deps.remove(instance.getId());

            return Pair.create(deps, range);
        }
        else
        {
            throw new IllegalArgumentException("Unsupported instance type: " + instance.getClass().getName());
        }
    }

    private Set<UUID> getCurrentDependencies(Instance instance, CfKey cfKey)
    {
        Lock lock = getCfKeyLock(cfKey);
        lock.lock();
        try
        {
            KeyState dm = loadKeyState(cfKey.key, cfKey.cfId);
            Set<UUID> deps = dm.getDepsAndAdd(instance.getId());
            saveKeyState(cfKey, dm);

            return deps;
        }
        finally
        {
            lock.unlock();
        }
    }

    public void recordMissingInstance(Instance instance)
    {
        CfKey cfKey;
        switch (instance.getType())
        {
            case QUERY:
                SerializedRequest request = ((QueryInstance) instance).getQuery();
                cfKey = request.getCfKey();
                recordMissingInstance(instance, cfKey);
                break;
            case TOKEN:
            case EPOCH:
                TokenState tokenState = tokenStateManager.recordMissingInstance((AbstractTokenInstance) instance);
                Iterator<CfKey> cfKeyIterator = getCfKeyIterator(tokenState, 10000);
                while (cfKeyIterator.hasNext())
                {
                    cfKey = cfKeyIterator.next();
                    recordMissingInstance(instance, cfKey);
                }
                break;
            default:
                throw new IllegalArgumentException("Unsupported instance type: " + instance.getClass().getName());
        }
    }

    private void recordMissingInstance(Instance instance, CfKey cfKey)
    {
        Lock lock = getCfKeyLock(cfKey);
        lock.lock();
        try
        {
            KeyState dm = loadKeyState(cfKey.key, cfKey.cfId);
            dm.recordInstance(instance.getId());

            if (instance.getState().atLeast(Instance.State.ACCEPTED))
            {
                dm.markAcknowledged(instance.getDependencies(), instance.getId());
            }

            saveKeyState(cfKey, dm);
        }
        finally
        {
            lock.unlock();
        }
    }

    public void recordAcknowledgedDeps(Instance instance)
    {
        CfKey cfKey;
        TokenState tokenState = tokenStateManager.get(instance);
        Range<Token> tokenRange = tokenStateManager.rangeFor(tokenState);
        switch (instance.getType())
        {
            case QUERY:
                SerializedRequest request = ((QueryInstance) instance).getQuery();
                cfKey = request.getCfKey();
                recordAcknowledgedDeps(instance, cfKey);
                break;
            case TOKEN:
                // since another instance has acknowledged this instance, it needs to be
                // kept around in the current epoch for failure recovery
                tokenStateManager.bindTokenInstanceToEpoch((TokenInstance) instance);
                tokenRange = ((TokenInstance) instance).getSplitRange();
            case EPOCH:
                Iterator<CfKey> cfKeyIterator = getCfKeyIterator(tokenRange, instance.getCfId(), 10000);
                while (cfKeyIterator.hasNext())
                {
                    cfKey = cfKeyIterator.next();
                    recordAcknowledgedDeps(instance, cfKey);
                }
                break;
            default:
                throw new IllegalArgumentException("Unsupported instance type: " + instance.getClass().getName());
        }
    }

    private void recordAcknowledgedDeps(Instance instance, CfKey cfKey)
    {
        Lock lock = getCfKeyLock(cfKey);
        lock.lock();
        try
        {
            KeyState dm = loadKeyState(cfKey.key, cfKey.cfId);
            dm.markAcknowledged(instance.getDependencies(), instance.getId());
            saveKeyState(cfKey, dm);
        }
        finally
        {
            lock.unlock();
        }
    }

    public void recordExecuted(Instance instance, ReplayPosition position, long maxTimestamp)
    {
        CfKey cfKey;
        TokenState tokenState = tokenStateManager.get(instance);
        Range<Token> tokenRange = tokenStateManager.rangeFor(tokenState);
        switch (instance.getType())
        {
            case QUERY:
                SerializedRequest request = ((QueryInstance) instance).getQuery();
                cfKey = request.getCfKey();
                recordExecuted(instance, cfKey, position, maxTimestamp);
                break;
            case TOKEN:
                tokenStateManager.bindTokenInstanceToEpoch((TokenInstance) instance);
                tokenRange = ((TokenInstance) instance).getSplitRange();
            case EPOCH:
                Iterator<CfKey> cfKeyIterator = getCfKeyIterator(tokenRange, instance.getCfId(), 10000);
                while (cfKeyIterator.hasNext())
                {
                    cfKey = cfKeyIterator.next();
                    recordExecuted(instance, cfKey, position, maxTimestamp);
                }
                break;
            default:
                throw new IllegalArgumentException("Unsupported instance type: " + instance.getClass().getName());
        }
    }

    private void recordExecuted(Instance instance, CfKey cfKey, ReplayPosition position, long maxTimestamp)
    {

        Lock lock = getCfKeyLock(cfKey);
        lock.lock();
        try
        {
            KeyState dm = loadKeyState(cfKey.key, cfKey.cfId);
            dm.markExecuted(instance.getId(), instance.getStronglyConnected(), position, maxTimestamp);
            if (instance.getType() == Instance.Type.QUERY)
            {
                dm.markQueryExecution();
            }
            saveKeyState(cfKey, dm);
        }
        finally
        {
            lock.unlock();
        }
    }

    public long getMaxTimestamp(CfKey cfKey)
    {
        Lock lock = getCfKeyLock(cfKey);
        lock.lock();
        try
        {
            KeyState dm = loadKeyState(cfKey.key, cfKey.cfId);
            return dm.getMaxTimestamp();
        }
        finally
        {
            lock.unlock();
        }
    }

    private void addTokenDeps(CfKey cfKey, KeyState keyState)
    {
        for (UUID id: tokenStateManager.getCurrentTokenDependencies(cfKey))
        {
            keyState.recordInstance(id);
        }
    }

    private KeyState deserialize(ByteBuffer data, int version)
    {
        try(DataInputStream in = new DataInputStream(ByteBufferUtil.inputStream(data)))
        {
            return KeyState.serializer.deserialize(in, version);
        }
        catch (IOException e)
        {
            throw new AssertionError(e);
        }
    }

    public KeyState loadKeyState(CfKey cfKey)
    {
        return loadKeyState(cfKey.key, cfKey.cfId, true);
    }

    public KeyState loadKeyState(CfKey cfKey, boolean createIfMissing)
    {
        return loadKeyState(cfKey.key, cfKey.cfId, createIfMissing);
    }

    public KeyState loadKeyStateIfExists(CfKey cfKey)
    {
        KeyState dm = cache.getIfPresent(cfKey);
        if (dm != null)
            return dm;

        String query = "SELECT * FROM %s.%s WHERE row_key=? AND cf_id=?";
        UntypedResultSet results = QueryProcessor.executeInternal(String.format(query, keyspace, table), cfKey.key, cfKey.cfId);

        if (results.isEmpty())
        {
            return null;
        }

        UntypedResultSet.Row row = results.one();

        dm = deserialize(row.getBlob("data"), row.getInt("version"));
        cache.put(cfKey, dm);
        return dm;
    }

    public boolean exists(CfKey cfKey)
    {
        String query = "SELECT * FROM %s.%s WHERE row_key=? AND cf_id=?";
        return cache.getIfPresent(cfKey) != null || !QueryProcessor.executeInternal(String.format(query, keyspace, table), cfKey.key, cfKey.cfId).isEmpty();
    }

    void maybeRepairKeyStateEpoch(ByteBuffer key, UUID cfId, KeyState ks)
    {
        long tsEpoch = tokenStateManager.getEpoch(key, cfId);
        if (tsEpoch > ks.getEpoch())
        {
            ks.setEpoch(tsEpoch);
        }
    }

    public KeyState loadKeyState(ByteBuffer key, UUID cfId)
    {
        return loadKeyState(key, cfId, true);
    }
    /**
     * loads a dependency manager. Must be called within a lock held for this key & cfid pair
     */
    @VisibleForTesting
    public KeyState loadKeyState(ByteBuffer key, UUID cfId, boolean createIfMissing)
    {
        CfKey cfKey = new CfKey(key, cfId);
        KeyState ks = cache.getIfPresent(cfKey);
        if (ks == null)
        {
            String query = "SELECT * FROM %s.%s WHERE row_key=? AND cf_id=? AND scope=?";
            UntypedResultSet results = QueryProcessor.executeInternal(String.format(query, keyspace, table), key, cfId, scope.ordinal());

            if (results.isEmpty() && !createIfMissing)
            {
                return null;
            }

            if (results.isEmpty())
            {
                ks = new KeyState(tokenStateManager.getEpoch(key, cfId));

                // add the current epoch dependencies if this is a new key
                addTokenDeps(cfKey, ks);
                saveKeyState(cfKey, ks);
            }
            else
            {
                UntypedResultSet.Row row = results.one();

                ks = deserialize(row.getBlob("data"), row.getInt("version"));
            }

            cache.put(cfKey, ks);
        }
        maybeRepairKeyStateEpoch(key, cfId, ks);
        return ks;
    }

    @VisibleForTesting
    boolean managesKey(ByteBuffer key, UUID cfId)
    {
        if (cache.getIfPresent(new CfKey(key, cfId)) != null)
        {
            return true;
        }
        else
        {
            String query = "SELECT * FROM %s.%s WHERE row_key=? AND cf_id=? AND scope=?";
            UntypedResultSet results = QueryProcessor.executeInternal(String.format(query, keyspace, table), key, cfId, scope.ordinal());
            return !results.isEmpty();
        }
    }

    void saveKeyState(CfKey cfKey, KeyState dm)
    {
        saveKeyState(cfKey.key, cfKey.cfId, dm, true);
    }

    void saveKeyState(ByteBuffer key, UUID cfId, KeyState dm)
    {
        saveKeyState(key, cfId, dm, true);
    }

    /**
     * persists a dependency manager. Must be called within a lock held for this key & cfid pair
     */
    void saveKeyState(ByteBuffer key, UUID cfId, KeyState dm, boolean cache)
    {

        DataOutputBuffer out = new DataOutputBuffer((int) KeyState.serializer.serializedSize(dm, EpaxosService.Version.CURRENT));
        try
        {
            KeyState.serializer.serialize(dm, out, EpaxosService.Version.CURRENT);
        }
        catch (IOException e)
        {
            throw new AssertionError(e);
        }
        String depsReq = "INSERT INTO %s.%s (row_key, cf_id, scope, version, data) VALUES (?, ?, ?, ?, ?)";
        QueryProcessor.executeInternal(String.format(depsReq, keyspace, table),
                                       key,
                                       cfId,
                                       scope.ordinal(),
                                       EpaxosService.Version.CURRENT,
                                       ByteBuffer.wrap(out.getData()));

        if (cache)
        {
            this.cache.put(new CfKey(key, cfId), dm);
        }
    }

    void deleteKeyState(CfKey cfKey)
    {
        cache.invalidate(cfKey);
        String deleteReq = "DELETE FROM %s.%s WHERE row_key=? AND cf_id=? AND scope=?";
        QueryProcessor.executeInternal(String.format(deleteReq, keyspace, table), cfKey.key, cfKey.cfId, scope.ordinal());
    }

    /**
     * Checks that none of the key managers owned by the given token state
     * have any active instances in any epochs but the current one
     */
    public boolean canIncrementToEpoch(TokenState tokenState, long targetEpoch)
    {
        Iterator<CfKey> cfKeyIterator = getCfKeyIterator(tokenState, 10000);
        while (cfKeyIterator.hasNext())
        {
            CfKey cfKey = cfKeyIterator.next();
            KeyState keyState = loadKeyState(cfKey.key, cfKey.cfId);
            if (!keyState.canIncrementToEpoch(targetEpoch))
            {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns true if we haven't been streamed any mutations
     * that are ahead of the current execution position
     */
    public boolean canExecute(CfKey cfKey)
    {
        Lock lock = getCfKeyLock(cfKey);
        lock.lock();
        try
        {
            KeyState ks = loadKeyState(cfKey.key, cfKey.cfId);
            return ks.canExecute();
        }
        finally
        {
            lock.unlock();
        }
    }

    /**
     * Reports that a row was streamed to us that contains a mutation
     * from an instance that hasn't been executed locally yet.
     */
    public boolean reportFutureRepair(CfKey cfKey, ExecutionInfo info)
    {
        Lock lock = getCfKeyLock(cfKey);
        lock.lock();
        try
        {
            KeyState ks = loadKeyState(cfKey.key, cfKey.cfId);
            return ks.setFutureExecution(info);
        }
        finally
        {
            lock.unlock();
        }
    }

    public ExecutionInfo getExecutionInfo(CfKey cfKey)
    {
        Lock lock = getCfKeyLock(cfKey);
        lock.lock();
        try
        {
            KeyState keyState = loadKeyState(cfKey, false);
            return keyState != null ? new ExecutionInfo(keyState.getEpoch(), keyState.getExecutionCount()) : null;
        }
        finally
        {
            lock.unlock();
        }
    }

    public boolean gcKeyState(CfKey cfKey)
    {
        Lock lock = getCfKeyLock(cfKey);
        lock.lock();
        try
        {
            KeyState keyState = loadKeyState(cfKey, false);
            // double check with lock aquired
            if (keyState.canGc(tokenStateManager.getCurrentTokenDependencies(cfKey)))
            {
                deleteKeyState(cfKey);
                return true;
            }
            return false;
        }
        finally
        {
            lock.unlock();
        }
    }

    /**
     * Returns execution info for the given token range and replay position. If the replay position is too far in the
     * past to have data, the execution position directly before the first known mutation is returned. This is ok since
     * it will be in a current epoch which we know about or are in the process of recovering.
     *
     * This reads the current key state data off of disk, bypassing the cache and locks. So the likelihood of getting
     * stale data is high. This exists to generate metadata for streams.
     */
    public Iterator<Pair<ByteBuffer, Map<Scope, ExecutionInfo>>> getRangeExecutionInfo(UUID cfId, Range<Token> range, final ReplayPosition replayPosition)
    {
        Function<UntypedResultSet.Row, Pair<ByteBuffer, Map<Scope, ExecutionInfo>>> f;
        f = new Function<UntypedResultSet.Row, Pair<ByteBuffer, Map<Scope, ExecutionInfo>>>()
        {
            public Pair<ByteBuffer, Map<Scope, ExecutionInfo>> apply(UntypedResultSet.Row row)
            {
                ByteBuffer key = row.getBlob("row_key");
                KeyState keyState = deserialize(row.getBlob("data"), row.getInt("version"));
                Map<Scope, ExecutionInfo> m = new EnumMap<>(Scope.class);
                m.put(scope, keyState.getExecutionInfoAtPosition(replayPosition));
                return Pair.create(key, m);
            }
        };
        Iterable<UntypedResultSet.Row> iterable;
        iterable = new KeyTableIterable(keyspace, table, range, true);
        iterable = Iterables.filter(iterable, new KeyTableIterable.CfIdPredicate(cfId));
        iterable = Iterables.filter(iterable, new KeyTableIterable.ScopePredicate(scope));
        return Iterables.transform(iterable, f).iterator();
    }

    /**
     * Returns key states for the purpose of post stream / failure recovery cleanup
     */
    public Iterator<KeyState> getStaleKeyStateRange(UUID cfId, Range<Token> range)
    {
        Function<UntypedResultSet.Row, KeyState> f = new Function<UntypedResultSet.Row, KeyState>()
        {
            @Override
            public KeyState apply(UntypedResultSet.Row row)
            {
                return deserialize(row.getBlob("data"), row.getInt("version"));
            }
        };
        Iterable<UntypedResultSet.Row> iterable;
        iterable = new KeyTableIterable(keyspace, table, range, true);
        iterable = Iterables.filter(iterable, new KeyTableIterable.CfIdPredicate(cfId));
        iterable = Iterables.filter(iterable, new KeyTableIterable.ScopePredicate(scope));
        return Iterables.transform(iterable, f).iterator();

    }
}
