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

package org.apache.cassandra.io.sstable.format;

import java.io.IOError;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.function.LongPredicate;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.compaction.CompactionController;
import org.apache.cassandra.db.compaction.CompactionInfo;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.IVerifier;
import org.apache.cassandra.io.sstable.KeyIterator;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.util.DataIntegrityMetadata;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.IFilter;
import org.apache.cassandra.utils.OutputHandler;
import org.apache.cassandra.utils.TimeUUID;

public abstract class SortedTableVerifier<R extends SSTableReader> implements IVerifier
{
    protected final ColumnFamilyStore cfs;
    protected final R sstable;

    protected final CompactionController controller;
    protected final ReadWriteLock fileAccessLock;
    protected final RandomAccessReader dataFile;
    protected final VerifyInfo verifyInfo;
    protected final Options options;
    protected final boolean isOffline;

    /**
     * Given a keyspace, return the set of local and pending token ranges.  By default {@link StorageService#getLocalAndPendingRanges(String)}
     * is expected, but for the standalone verifier case we can't use that, so this is here to allow the CLI to provide
     * the token ranges.
     */
    protected final Function<String, ? extends Collection<Range<Token>>> tokenLookup;
    protected int goodRows;

    protected final OutputHandler outputHandler;
    protected DataIntegrityMetadata.FileDigestValidator validator;

    public SortedTableVerifier(ColumnFamilyStore cfs, R sstable, OutputHandler outputHandler, boolean isOffline, Options options)
    {
        this.cfs = cfs;
        this.sstable = sstable;
        this.outputHandler = outputHandler;

        this.controller = new VerifyController(cfs);

        this.fileAccessLock = new ReentrantReadWriteLock();
        this.dataFile = isOffline
                        ? sstable.openDataReader()
                        : sstable.openDataReader(CompactionManager.instance.getRateLimiter());
        this.verifyInfo = new VerifyInfo(dataFile, sstable, fileAccessLock.readLock());
        this.options = options;
        this.isOffline = isOffline;
        this.tokenLookup = options.tokenLookup;
    }

    protected void deserializeBloomFilter(SSTableReader sstable) throws IOException
    {
        IFilter filter = FilterComponent.load(sstable.descriptor);
        if (filter != null)
            filter.close();
    }

    public CompactionInfo.Holder getVerifyInfo()
    {
        return verifyInfo;
    }

    protected static void throwIfFatal(Throwable th)
    {
        if (th instanceof Error && !(th instanceof AssertionError || th instanceof IOError))
            throw (Error) th;
    }

    protected void markAndThrow(Throwable cause)
    {
        markAndThrow(cause, true);
    }

    protected void markAndThrow(Throwable cause, boolean mutateRepaired)
    {
        if (mutateRepaired && options.mutateRepairStatus) // if we are able to mutate repaired flag, an incremental repair should be enough
        {
            try
            {
                sstable.mutateRepairedAndReload(ActiveRepairService.UNREPAIRED_SSTABLE, sstable.getPendingRepair(), sstable.isTransient());
                cfs.getTracker().notifySSTableRepairedStatusChanged(Collections.singleton(sstable));
            }
            catch (IOException ioe)
            {
                outputHandler.output("Error mutating repairedAt for SSTable %s, as part of markAndThrow", sstable.getFilename());
            }
        }
        Exception e = new Exception(String.format("Invalid SSTable %s, please force %srepair", sstable.getFilename(), (mutateRepaired && options.mutateRepairStatus) ? "" : "a full "), cause);
        if (options.invokeDiskFailurePolicy)
            throw new CorruptSSTableException(e, sstable.getFilename());
        else
            throw new RuntimeException(e);
    }

    protected void verifyBloomFilter()
    {
        try
        {
            outputHandler.debug("Deserializing bloom filter for %s", sstable);
            deserializeBloomFilter(sstable);
        }
        catch (Throwable t)
        {
            outputHandler.warn(t);
            markAndThrow(t);
        }
    }

    protected void verifySSTableMetadata()
    {
        outputHandler.output("Deserializing sstable metadata for %s ", sstable);
        try
        {
            StatsComponent statsComponent = StatsComponent.load(sstable.descriptor, MetadataType.VALIDATION, MetadataType.STATS, MetadataType.HEADER);
            if (statsComponent.validationMetadata() != null &&
                !statsComponent.validationMetadata().partitioner.equals(sstable.getPartitioner().getClass().getCanonicalName()))
                throw new IOException("Partitioner does not match validation metadata");
        }
        catch (Throwable t)
        {
            outputHandler.warn(t);
            markAndThrow(t, false);
        }
    }

    protected void verifySSTableVersion()
    {
        outputHandler.output("Verifying %s (%s)", sstable, FBUtilities.prettyPrintMemory(dataFile.length()));
        if (options.checkVersion && !sstable.descriptor.version.isLatestVersion())
        {
            String msg = String.format("%s is not the latest version, run upgradesstables", sstable);
            outputHandler.output(msg);
            // don't use markAndThrow here because we don't want a CorruptSSTableException for this.
            throw new RuntimeException(msg);
        }
    }

    protected int verifyOwnedRanges()
    {
        List<Range<Token>> ownedRanges = Collections.emptyList();
        outputHandler.debug("Checking that all tokens are owned by the current node");
        try (KeyIterator iter = sstable.keyIterator())
        {
            ownedRanges = Range.normalize(tokenLookup.apply(cfs.metadata.keyspace));
            if (ownedRanges.isEmpty())
                return 0;
            RangeOwnHelper rangeOwnHelper = new RangeOwnHelper(ownedRanges);
            while (iter.hasNext())
            {
                DecoratedKey key = iter.next();
                rangeOwnHelper.validate(key);
            }
        }
        catch (Throwable t)
        {
            outputHandler.warn(t);
            markAndThrow(t);
        }

        return ownedRanges.size();
    }

    protected boolean verifyDigest()
    {
        boolean passed = true;
        // Verify will use the Digest files, which works for both compressed and uncompressed sstables
        outputHandler.output("Checking computed hash of %s ", sstable);
        try
        {
            validator = null;

            if (new File(sstable.descriptor.filenameFor(Component.DIGEST)).exists())
            {
                validator = DataIntegrityMetadata.fileDigestValidator(sstable.descriptor);
                validator.validate();
            }
            else
            {
                outputHandler.output("Data digest missing, assuming extended verification of disk values");
                passed = false;
            }
        }
        catch (IOException e)
        {
            outputHandler.warn(e);
            markAndThrow(e);
        }
        finally
        {
            FileUtils.closeQuietly(validator);
        }
        return passed;
    }

    /**
     * Use the fact that check(...) is called with sorted tokens - we keep a pointer in to the normalized ranges
     * and only bump the pointer if the key given is out of range. This is done to avoid calling .contains(..) many
     * times for each key (with vnodes for example)
     */
    @VisibleForTesting
    public static class RangeOwnHelper
    {
        private final List<Range<Token>> normalizedRanges;
        private int rangeIndex = 0;
        private DecoratedKey lastKey;

        public RangeOwnHelper(List<Range<Token>> normalizedRanges)
        {
            this.normalizedRanges = normalizedRanges;
            Range.assertNormalized(normalizedRanges);
        }

        /**
         * check if the given key is contained in any of the given ranges
         * <p>
         * Must be called in sorted order - key should be increasing
         *
         * @param key the key
         * @throws RuntimeException if the key is not contained
         */
        public void validate(DecoratedKey key)
        {
            if (!check(key))
                throw new RuntimeException("Key " + key + " is not contained in the given ranges");
        }

        /**
         * check if the given key is contained in any of the given ranges
         * <p>
         * Must be called in sorted order - key should be increasing
         *
         * @param key the key
         * @return boolean
         */
        public boolean check(DecoratedKey key)
        {
            assert lastKey == null || key.compareTo(lastKey) > 0;
            lastKey = key;

            if (normalizedRanges.isEmpty()) // handle tests etc. where we don't have any ranges
                return true;

            if (rangeIndex > normalizedRanges.size() - 1)
                throw new IllegalStateException("RangeOwnHelper can only be used to find the first out-of-range-token");

            while (!normalizedRanges.get(rangeIndex).contains(key.getToken()))
            {
                rangeIndex++;
                if (rangeIndex > normalizedRanges.size() - 1)
                    return false;
            }

            return true;
        }
    }

    protected static class VerifyInfo extends CompactionInfo.Holder
    {
        private final RandomAccessReader dataFile;
        private final SSTableReader sstable;
        private final TimeUUID verificationCompactionId;
        private final Lock fileReadLock;

        public VerifyInfo(RandomAccessReader dataFile, SSTableReader sstable, Lock fileReadLock)
        {
            this.dataFile = dataFile;
            this.sstable = sstable;
            this.fileReadLock = fileReadLock;
            verificationCompactionId = TimeUUID.Generator.nextTimeUUID();
        }

        public CompactionInfo getCompactionInfo()
        {
            fileReadLock.lock();
            try
            {
                return new CompactionInfo(sstable.metadata(),
                                          OperationType.VERIFY,
                                          dataFile.getFilePointer(),
                                          dataFile.length(),
                                          verificationCompactionId,
                                          ImmutableSet.of(sstable));
            }
            catch (Exception e)
            {
                throw new RuntimeException();
            }
            finally
            {
                fileReadLock.unlock();
            }
        }

        public boolean isGlobal()
        {
            return false;
        }
    }

    protected static class VerifyController extends CompactionController
    {
        public VerifyController(ColumnFamilyStore cfs)
        {
            super(cfs, Integer.MAX_VALUE);
        }

        @Override
        public LongPredicate getPurgeEvaluator(DecoratedKey key)
        {
            return time -> false;
        }
    }
}