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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.function.LongPredicate;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.compaction.CompactionController;
import org.apache.cassandra.db.compaction.CompactionInfo;
import org.apache.cassandra.db.compaction.CompactionInterruptedException;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.IVerifier;
import org.apache.cassandra.io.sstable.KeyIterator;
import org.apache.cassandra.io.sstable.KeyReader;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.util.DataIntegrityMetadata;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.IFilter;
import org.apache.cassandra.utils.OutputHandler;
import org.apache.cassandra.utils.TimeUUID;

public abstract class SortedTableVerifier<R extends SSTableReaderWithFilter> implements IVerifier
{
    private final static Logger logger = LoggerFactory.getLogger(SortedTableVerifier.class);

    protected final ColumnFamilyStore cfs;
    protected final R sstable;

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

    public SortedTableVerifier(ColumnFamilyStore cfs, R sstable, OutputHandler outputHandler, boolean isOffline, Options options)
    {
        this.cfs = cfs;
        this.sstable = sstable;
        this.outputHandler = outputHandler;

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
        try (IFilter filter = FilterComponent.load(sstable.descriptor)) {
            if (filter != null)
                logger.trace("Filter loaded for {}", sstable);
        }
    }

    public CompactionInfo.Holder getVerifyInfo()
    {
        return verifyInfo;
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

    public void verify()
    {
        verifySSTableVersion();

        verifySSTableMetadata();

        verifyIndex();

        verifyBloomFilter();

        if (options.checkOwnsTokens && !isOffline && !(cfs.getPartitioner() instanceof LocalPartitioner))
        {
            if (verifyOwnedRanges() == 0)
                return;
        }

        if (options.quick)
            return;

        if (verifyDigest() && !options.extendedVerification)
            return;

        verifySSTable();

        outputHandler.output("Verify of %s succeeded. All %d rows read successfully", sstable, goodRows);
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
            DataIntegrityMetadata.FileDigestValidator validator = sstable.maybeGetDigestValidator();

            if (validator != null)
            {
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
        return passed;
    }

    protected void verifySSTable()
    {
        outputHandler.output("Extended Verify requested, proceeding to inspect values");

        try (VerifyController verifyController = new VerifyController(cfs);
             KeyReader indexIterator = sstable.keyReader())
        {
            if (indexIterator.dataPosition() != 0)
                markAndThrow(new RuntimeException("First row position from index != 0: " + indexIterator.dataPosition()));

            List<Range<Token>> ownedRanges = isOffline ? Collections.emptyList() : Range.normalize(tokenLookup.apply(cfs.metadata().keyspace));
            RangeOwnHelper rangeOwnHelper = new RangeOwnHelper(ownedRanges);
            DecoratedKey prevKey = null;

            while (!dataFile.isEOF())
            {

                if (verifyInfo.isStopRequested())
                    throw new CompactionInterruptedException(verifyInfo.getCompactionInfo());

                long rowStart = dataFile.getFilePointer();
                outputHandler.debug("Reading row at %d", rowStart);

                DecoratedKey key = null;
                try
                {
                    key = sstable.decorateKey(ByteBufferUtil.readWithShortLength(dataFile));
                }
                catch (Throwable th)
                {
                    markAndThrow(th);
                }

                if (options.checkOwnsTokens && ownedRanges.size() > 0 && !(cfs.getPartitioner() instanceof LocalPartitioner))
                {
                    try
                    {
                        rangeOwnHelper.validate(key);
                    }
                    catch (Throwable t)
                    {
                        outputHandler.warn(t, "Key %s in sstable %s not owned by local ranges %s", key, sstable, ownedRanges);
                        markAndThrow(t);
                    }
                }

                ByteBuffer currentIndexKey = indexIterator.key();
                long nextRowPositionFromIndex = 0;
                try
                {
                    nextRowPositionFromIndex = indexIterator.advance()
                                               ? indexIterator.dataPosition()
                                               : dataFile.length();
                }
                catch (Throwable th)
                {
                    markAndThrow(th);
                }

                long dataStart = dataFile.getFilePointer();
                long dataStartFromIndex = currentIndexKey == null
                                          ? -1
                                          : rowStart + 2 + currentIndexKey.remaining();

                long dataSize = nextRowPositionFromIndex - dataStartFromIndex;
                // avoid an NPE if key is null
                String keyName = key == null ? "(unreadable key)" : ByteBufferUtil.bytesToHex(key.getKey());
                outputHandler.debug("row %s is %s", keyName, FBUtilities.prettyPrintMemory(dataSize));

                try
                {
                    if (key == null || dataSize > dataFile.length())
                        markAndThrow(new RuntimeException(String.format("key = %s, dataSize=%d, dataFile.length() = %d", key, dataSize, dataFile.length())));

                    try (UnfilteredRowIterator iterator = SSTableIdentityIterator.create(sstable, dataFile, key))
                    {
                        verifyPartition(key, iterator);
                    }

                    if ((prevKey != null && prevKey.compareTo(key) > 0) || !key.getKey().equals(currentIndexKey) || dataStart != dataStartFromIndex)
                        markAndThrow(new RuntimeException("Key out of order: previous = " + prevKey + " : current = " + key));

                    goodRows++;
                    prevKey = key;


                    outputHandler.debug("Row %s at %s valid, moving to next row at %s ", goodRows, rowStart, nextRowPositionFromIndex);
                    dataFile.seek(nextRowPositionFromIndex);
                }
                catch (Throwable th)
                {
                    markAndThrow(th);
                }
            }
        }
        catch (Throwable t)
        {
            Throwables.throwIfUnchecked(t);
            throw new RuntimeException(t);
        }
    }

    protected abstract void verifyPartition(DecoratedKey key, UnfilteredRowIterator iterator);

    protected void verifyIndex()
    {
        try
        {
            outputHandler.debug("Deserializing index for %s", sstable);
            deserializeIndex(sstable);
        }
        catch (Throwable t)
        {
            outputHandler.warn(t);
            markAndThrow(t);
        }
    }

    private void deserializeIndex(SSTableReader sstable) throws IOException
    {
        try (KeyReader it = sstable.keyReader())
        {
            ByteBuffer last = it.key();
            while (it.advance()) last = it.key(); // no-op, just check if index is readable
            if (!Objects.equals(last, sstable.getLast().getKey()))
                throw new CorruptSSTableException(new IOException("Failed to read partition index"), it.toString());
        }
    }

    @Override
    public void close()
    {
        fileAccessLock.writeLock().lock();
        try
        {
            FileUtils.closeQuietly(dataFile);
        }
        finally
        {
            fileAccessLock.writeLock().unlock();
        }
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
