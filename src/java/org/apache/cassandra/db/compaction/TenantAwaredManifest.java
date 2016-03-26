package org.apache.cassandra.db.compaction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;

public class TenantAwaredManifest
{
    private static final Logger logger = LoggerFactory.getLogger(TenantAwaredManifest.class);

    private final ColumnFamilyStore cfs;
    @VisibleForTesting
    protected final Map<String, List<SSTableReader>> tenants;
    protected final Map<String, AbstractCompactionStrategy> strategies;

    public TenantAwaredManifest(ColumnFamilyStore cfs)
    {
        this.cfs = cfs;
        this.tenants = new ConcurrentHashMap<>();
        strategies = new HashMap<>();
    }

    public static TenantAwaredManifest create(ColumnFamilyStore cfs, Iterable<SSTableReader> sstables)
    {
        TenantAwaredManifest manifest = new TenantAwaredManifest(cfs);

        // ensure all SSTables are in the manifest
        for (SSTableReader ssTableReader : sstables)
        {
            manifest.add(ssTableReader);
        }
        return manifest;
    }

    private List<SSTableReader> getTenantGroupOrCreate(String tenantId)
    {
        if (tenants.get(tenantId) == null)
        {
            tenants.put(tenantId, new ArrayList<SSTableReader>());
        }
        return tenants.get(tenantId);
    }

    private AbstractCompactionStrategy getStrategyOrCreate(String tenantId)
    { 
        if (strategies.get(tenantId) == null)
        {
            strategies.put(tenantId, new SizeTieredCompactionStrategy(cfs, new HashMap<String, String>()));
        }
        return strategies.get(tenantId);
    }

    public synchronized void add(SSTableReader reader)
    {
        String tenantId = reader.getSSTableTenant();

        logger.debug("Adding {} to Group:{}", reader, tenantId);
        getTenantGroupOrCreate(tenantId).add(reader);
        getStrategyOrCreate(tenantId).addSSTable(reader);

        logDistribution();
    }

    /**
     * @return highest-priority sstables to compact, and level to compact them to If no compactions are necessary, will
     *         return null
     */
    public synchronized AbstractCompactionTask getCompactionCandidates(int gcBefore)
    {
        String tenantId = findLargestUncompactingGroup();
        Set<SSTableReader> candidates = getCandidatesFor(tenantId);

        if (candidates.isEmpty())
        {
            return null;
        }

        if (tenantId.equals(TenantUtil.DEFAULT_TENANT))
        {
            List<SSTableReader> toBeSplit = Arrays.asList(candidates.iterator().next());
            if (cfs.getDataTracker().markCompacting(toBeSplit))
            {
                TenantAwaredSplitCompactionTask newTask = new TenantAwaredSplitCompactionTask(cfs, toBeSplit,
                        tenantId, true, gcBefore);
                return newTask;
            }
            else
            {
                return null;
            }
        }
        else
        {
            return getStrategyOrCreate(tenantId).getNextBackgroundTask(gcBefore);
        }

        // return new CompactionCandidate(candidates, tenantId, false,
        // cfs.getCompactionStrategy().getMaxSSTableBytes());
    }

    public synchronized int getEstimatedTasks()
    {
        long tasks = 0;
        long[] estimated = new long[tenants.keySet().size()];
        int index = 0;
        for (String tenantId : tenants.keySet())
        {
            List<SSTableReader> sstables = getTenantGroupOrCreate(tenantId);
            // If there is 1 byte over TBL - (MBL * 1.001), there is still a task left, so we need to round up.
            estimated[index] = sstables.size() > 1 ? 1 : 0;
            tasks += estimated[index];
            index++;
        }

        logger.debug("Estimating {} compactions to do for {}.{}",
                Arrays.toString(estimated), cfs.keyspace.getName(), cfs.name);
        return Ints.checkedCast(tasks);
    }
    private void logDistribution()
    {
        if (logger.isDebugEnabled())
        {
            for (Entry<String, List<SSTableReader>> entry : tenants.entrySet())
            {
                if (entry.getValue() != null && !entry.getValue().isEmpty())
                {
                    logger.debug("Group-{} contains {} SSTables ({} bytes) in {}",
                            entry.getValue(), entry.getValue().size(), SSTableReader.getTotalBytes(entry.getValue()),
                            this);
                }
            }
        }
    }

    @VisibleForTesting
    public String remove(SSTableReader reader)
    {
        String tenantId = reader.getSSTableTenant();

        getTenantGroupOrCreate(tenantId).remove(reader);
        getStrategyOrCreate(tenantId).addSSTable(reader);

        return tenantId;
    }

    private Set<SSTableReader> getCandidatesFor(String tenantId)
    {
        return new HashSet<>(getTenantGroupOrCreate(tenantId));
    }

    private String findLargestUncompactingGroup()
    {
        String tenantId = TenantUtil.DEFAULT_TENANT;
        int size = 0;
        for (Entry<String, List<SSTableReader>> entry : tenants.entrySet())
        {
            Set<SSTableReader> uncompacting = Sets.difference(new HashSet<>(entry.getValue()),
                    cfs.getDataTracker().getCompacting());
            if (uncompacting.size() > size)
            {
                tenantId = entry.getKey();
                size = uncompacting.size();
            }
        }
        return tenantId;
    }

    private Set<SSTableReader> getCompacting(String tenantId)
    {
        Set<SSTableReader> sstables = new HashSet<>();
        Set<SSTableReader> levelSSTables = new HashSet<>(getTenantGroupOrCreate(tenantId));
        for (SSTableReader sstable : cfs.getDataTracker().getCompacting())
        {
            if (levelSSTables.contains(sstable))
                sstables.add(sstable);
        }
        return sstables;
    }

    private List<SSTableReader> ageSortedSSTables(Collection<SSTableReader> candidates)
    {
        List<SSTableReader> ageSortedCandidates = new ArrayList<SSTableReader>(candidates);
        Collections.sort(ageSortedCandidates, SSTableReader.maxTimestampComparator);
        return ageSortedCandidates;
    }

    @Override
    public String toString()
    {
        return "Manifest@" + hashCode();
    }

    public static class CompactionCandidate
    {
        public final Collection<SSTableReader> sstables;
        public final String tenantGroup;
        public final long maxSSTableBytes;
        public final boolean shouldSplit;

        public CompactionCandidate(Collection<SSTableReader> sstables, String tenantGroup, boolean shouldSplit,
                long maxSSTableBytes)
        {
            this.sstables = sstables;
            this.tenantGroup = tenantGroup;
            this.maxSSTableBytes = maxSSTableBytes;
            this.shouldSplit = shouldSplit;
        }
    }
}
