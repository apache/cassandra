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

package org.apache.cassandra.harry.stress;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.HashMap;

import accord.utils.Invariants;
import org.apache.cassandra.harry.SchemaSpec;
import org.apache.cassandra.harry.gen.Generator;
import org.apache.cassandra.harry.gen.rng.SeedableEntropySource;
import org.apache.cassandra.harry.stress.distribution.Distribution;
import org.apache.cassandra.harry.util.ByteUtils;
import org.apache.cassandra.harry.util.TokenUtil;

/**
 * In order to generate SSTables per level, we ideally need to know partitions ahead of time, since otherwise
 * we need to keep SSTableWriters open, which is not feasible. To do this, we iterate through all LTS that will
 * be visited, and generate a mapping of Token / partition descriptor to set of LTS that will be visited for that
 * partition.
 *
 * Token index files are sorted in memory and flushed on disk in token order. After all LTS were replayed, we
 * merge-iterate through individual token index files and create one big merged file, in token order.
 */
public class TokenIndexGenerator
{
    static class TokenAndPd implements Comparable<TokenAndPd>
    {
        final long token;
        final long pd;

        TokenAndPd(long token, long pd)
        {
            this.token = token;
            this.pd = pd;
        }

        @Override
        public int compareTo(TokenAndPd o)
        {
            int cmp = Long.compare(token, o.token);
            return cmp != 0 ? cmp : Long.compare(pd, o.pd);
        }

        @Override
        public boolean equals(Object o)
        {
            if (!(o instanceof TokenAndPd)) return false;
            TokenAndPd that = (TokenAndPd) o;
            return token == that.token && pd == that.pd;
        }

        @Override
        public int hashCode()
        {
            return Long.hashCode(token) * 31 + Long.hashCode(pd);
        }
    }

    public static void generate(File dir,
                                SchemaSpec schema,
                                RotationStrategy rotationStrategy,
                                Distribution rowPopulation,
                                Generator<VisitGenerator.VisitType> visitTypeGen,
                                Distribution visitSizeDistribution,
                                long initialLts,
                                long visits) throws IOException
    {
        System.out.println("******************** SStable Level Generator ********************");
        System.out.println(String.format("  Output Directory: %s", dir));
        System.out.println(String.format("  Initial LTS: %d", initialLts));
        System.out.println(String.format("  Visits: %d", visits));
        System.out.println(String.format("  Rotation: %s", rotationStrategy));
        System.out.println();
        int fileIdx = 0;

        long nextPartitionIdx = 0;

        class TokenCache
        {
            private final HashMap<Long, Long> map = new HashMap<>();
            private final long[] ring;
            private int pos = 0;
            private boolean full = false;

            TokenCache(int capacity)
            {
                this.ring = new long[capacity];
            }

            long tokenFor(long partitionIdx)
            {
                Long cached = map.get(partitionIdx);
                if (cached != null)
                    return cached;

                long pd = ActivePartition.DescriptorIndexBijection.INSTANCE.toPd(partitionIdx);
                Object[] pkValues = SeedableEntropySource.computeWithSeed(pd, SchemaSpec.forKeys(schema.partitionKeys)::generate);
                long token = TokenUtil.token(ByteUtils.compose(ByteUtils.objectsToBytes(pkValues)));

                if (full)
                    map.remove(ring[pos]);
                ring[pos] = partitionIdx;
                pos = (pos + 1) % ring.length;
                if (pos == 0) full = true;
                map.put(partitionIdx, token);
                return token;
            }
        }

        TokenCache tokenCache = new TokenCache(10_000);
        ActivePartition.VisitedPartitions visitedPartitions = new ActivePartition.VisitedPartitions(0);
        long[] activePartitionIdxs = new long[rotationStrategy.targetSize()];
        Set<Long> activePartitionIdxSet = new HashSet<>(activePartitionIdxs.length);
        Map<Long, Integer> ltsPerPd = new HashMap<>();
        Map<Long, Integer> sizePerPd = new HashMap<>();

        // Initial active set fill. Do NOT pre-add to visitedPartitions: ActivePartition.Partitions.populate()
        // only adds an idx to visitedPartitions when it's evicted by a rotation, never at fill time.
        for (int i = 0; i < rotationStrategy.targetSize(); i++)
        {
            long idx = nextPartitionIdx++;
            long pd = ActivePartition.DescriptorIndexBijection.INSTANCE.toPd(idx);
            activePartitionIdxs[i] = idx;
            activePartitionIdxSet.add(idx);
            sizePerPd.putIfAbsent(pd, Math.toIntExact(rowPopulation.next(pd)));
        }

        TreeMap<TokenAndPd, ArrayList<Long>> visitsPerToken = new TreeMap<>();
        AtomicInteger cnt = new AtomicInteger();
        List<File> files = new ArrayList<>();
        for (long lts = 0; lts < initialLts + visits; lts++)
        {
            if (lts % 100_000 == 0)
                System.out.println("Seen " + lts);

            // Visit at lts is generated against state from rotations [< lts] (matching VisitGenerator.inflate +
            // HarryStress.start, where visit(L) is generated before maybeSwitchPartition(L) is called).
            if (lts >= initialLts)
            {
                VisitGenerator.VisitType visitType = SeedableEntropySource.computeWithSeed(lts, visitTypeGen::generate);
                if (visitType == VisitGenerator.VisitType.MUTATE)
                {
                    long visitSize = visitSizeDistribution.next(lts);
                    Invariants.require(visitSize > 0);
                    for (int op = 0; op < visitSize; op++)
                    {
                        // If you are changing choosing here, make sure to also change VisitGeneratort#mutationVisit
                        int slot = SeedableEntropySource.computeWithSeed(lts, ~op, r -> r.nextInt(activePartitionIdxs.length));
                        long partitionIdx = activePartitionIdxs[slot];
                        long pd = ActivePartition.DescriptorIndexBijection.INSTANCE.toPd(partitionIdx);
                        long token = tokenCache.tokenFor(partitionIdx);
                        visitsPerToken.computeIfAbsent(new TokenAndPd(token, pd), k -> new ArrayList<>(10))
                                      .add(lts);
                        ltsPerPd.merge(pd, 1, Integer::sum);
                        cnt.incrementAndGet();
                    }
                }
            }

            // Rotation. Skip at lts==initialLts: populate covers [0, initialLts) and HarryStress.start's first
            // maybeSwitchPartition is at initialLts+1, so shouldSwitch(initialLts) is never called by runtime.
            if (lts != initialLts && rotationStrategy.shouldSwitch(lts))
            {
                RotationStrategy.PartitionAction[] actions = SeedableEntropySource.computeWithSeed(lts, rotationStrategy::generate);
                for (int i = 0; i < actions.length; i++)
                {
                    RotationStrategy.PartitionAction action = actions[i];
                    int remove = SeedableEntropySource.computeWithSeed(Util.hash(lts, i), r -> r.nextInt(activePartitionIdxs.length));
                    long toVisitedIdx = activePartitionIdxs[remove];
                    long toVisitedPd = ActivePartition.DescriptorIndexBijection.INSTANCE.toPd(toVisitedIdx);
                    int partitionSize = Math.toIntExact(rowPopulation.next(toVisitedPd));
                    sizePerPd.putIfAbsent(toVisitedPd, partitionSize);
                    boolean evict = SeedableEntropySource.computeWithSeed(Util.hash(lts, i), r -> r.nextInt(Math.max(1, Integer.highestOneBit(partitionSize))) == 0);
                    if (!evict) continue;
                    switch (action)
                    {
                        case REPLACE_WITH_NEW:
                        {
                            long newIdx = nextPartitionIdx++;
                            long newPd = ActivePartition.DescriptorIndexBijection.INSTANCE.toPd(newIdx);
                            sizePerPd.putIfAbsent(newPd, Math.toIntExact(rowPopulation.next(newPd)));
                            activePartitionIdxSet.remove(toVisitedIdx);
                            activePartitionIdxs[remove] = newIdx;
                            activePartitionIdxSet.add(newIdx);
                            break;
                        }
                        case REPLACE_WITH_VISITED:
                        {
                            long toRevisitIdx = visitedPartitions.getBySeed(Util.hash(lts, i));
                            if (toRevisitIdx < 0 || activePartitionIdxSet.contains(toRevisitIdx))
                            {
                                // picked one is still active; fall back to new
                                long newIdx = nextPartitionIdx++;
                                long newPd = ActivePartition.DescriptorIndexBijection.INSTANCE.toPd(newIdx);
                                sizePerPd.putIfAbsent(newPd, Math.toIntExact(rowPopulation.next(newPd)));
                                activePartitionIdxSet.remove(activePartitionIdxs[remove]);
                                activePartitionIdxs[remove] = newIdx;
                                activePartitionIdxSet.add(newIdx);
                            }
                            else
                            {
                                activePartitionIdxSet.remove(toVisitedIdx);
                                activePartitionIdxs[remove] = toRevisitIdx;
                                activePartitionIdxSet.add(toRevisitIdx);
                            }
                            break;
                        }
                    }
                    visitedPartitions.add(toVisitedIdx);
                }
            }

            if (visitsPerToken.size() > 100000 || lts == initialLts + visits - 1)
            {
                File currentFile = new File(dir, "tokens_" + fileIdx++);
                System.out.println("Writing " + currentFile);
                currentFile.createNewFile();
                files.add(currentFile);
                try (FileOutputStream s = new FileOutputStream(currentFile);
                     BufferedOutputStream os = new BufferedOutputStream(s);
                     DataOutputStream dos = new DataOutputStream(os))
                {
                    for (Map.Entry<TokenAndPd, ArrayList<Long>> entry : visitsPerToken.entrySet())
                    {
                        dos.writeLong(entry.getKey().token);
                        dos.writeLong(entry.getKey().pd);
                        dos.writeInt(entry.getValue().size());
                        for (Long l : entry.getValue())
                            dos.writeLong(l);
                    }
                }
                visitsPerToken.clear();
            }
        }
        merge(files, new File(dir, "merged_tokens"));

        printHistogram("LTS per partition", ltsPerPd.values());
        printHistogram("Partition size", sizePerPd.values());
    }

    private static final int[] BOUNDARIES = { 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 100000 };

    static void printHistogram(String label, Collection<Integer> values)
    {
        int[] buckets = new int[BOUNDARIES.length + 1];
        for (int v : values)
        {
            int b = 0;
            while (b < BOUNDARIES.length && v >= BOUNDARIES[b])
                b++;
            buckets[b]++;
        }
        System.out.println(String.format("\n%s distribution (%d entries):", label, values.size()));
        System.out.println(String.format("  [0-%d): %d", BOUNDARIES[0], buckets[0]));
        for (int i = 1; i < BOUNDARIES.length; i++)
        {
            if (buckets[i] > 0)
                System.out.println(String.format("  [%d-%d): %d", BOUNDARIES[i - 1], BOUNDARIES[i], buckets[i]));
        }
        System.out.println(String.format("  [%d+): %d", BOUNDARIES[BOUNDARIES.length - 1], buckets[BOUNDARIES.length]));
    }

    /**
     * Merges token files produced by {@link #generate}.
     * If the same (token, pd) appears in multiple files, LTS bytes are concatenated in file index order.
     * Also writes an.idx file with [token:8][offset:8] entries (one per unique token).
     */
    public static void merge(List<File> inputFiles, File outputFile) throws IOException
    {
        class PeekingIter implements Comparable<PeekingIter>, Closeable
        {
            private final FileInputStream fis;
            private final FileChannel channel;
            private final int fileIndex;
            private final ByteBuffer headerBuf = ByteBuffer.allocate(20); // token(8) + pd(8) + count(4)
            long currentToken;
            long currentPd;
            int currentLtsCount;
            boolean hasMore;

            PeekingIter(FileInputStream fis, int fileIndex) throws IOException
            {
                this.fis = fis;
                this.channel = fis.getChannel();
                this.fileIndex = fileIndex;
                advance();
            }

            void advance() throws IOException
            {
                headerBuf.clear();
                while (headerBuf.hasRemaining())
                {
                    int read = channel.read(headerBuf);
                    if (read < 0)
                    {
                        hasMore = false;
                        return;
                    }
                }
                headerBuf.flip();
                currentToken = headerBuf.getLong();
                currentPd = headerBuf.getLong();
                currentLtsCount = headerBuf.getInt();
                hasMore = true;
            }

            void appendTo(FileChannel out) throws IOException
            {
                long bytes = (long) currentLtsCount * Long.BYTES;
                long pos = channel.position();
                long remaining = bytes;
                while (remaining > 0)
                {
                    long transferred = channel.transferTo(pos, remaining, out);
                    pos += transferred;
                    remaining -= transferred;
                }
                channel.position(pos);
            }

            @Override
            public int compareTo(PeekingIter other)
            {
                int cmp = Long.compare(this.currentToken, other.currentToken);
                if (cmp != 0) return cmp;
                cmp = Long.compare(this.currentPd, other.currentPd);
                if (cmp != 0) return cmp;
                return Integer.compare(this.fileIndex, other.fileIndex);
            }

            @Override
            public void close() throws IOException
            {
                channel.close();
                fis.close();
            }
        }

        ArrayList<PeekingIter> readers = new ArrayList<>();
        PriorityQueue<PeekingIter> pq = new PriorityQueue<>();
        for (int i = 0; i < inputFiles.size(); i++)
        {
            @SuppressWarnings("IOResourceOpenedButNotSafelyClosed") FileInputStream fis = new FileInputStream(inputFiles.get(i));
            PeekingIter reader = new PeekingIter(fis, i);
            readers.add(reader);
            if (reader.hasMore)
                pq.add(reader);
        }

        System.out.println("Writing merged results to " + outputFile);
        try (FileOutputStream fos = new FileOutputStream(outputFile);
             FileChannel out = fos.getChannel();
             FileOutputStream idxFos = new FileOutputStream(outputFile.getPath() + ".idx");
             FileChannel idx = idxFos.getChannel())
        {
            ByteBuffer headerBuf = ByteBuffer.allocate(20); // token(8) + pd(8) + count(4)
            ByteBuffer idxBuf = ByteBuffer.allocate(16);    // token(8) + offset(8)
            long lastIndexedToken = Long.MIN_VALUE;
            boolean firstEntry = true;

            while (!pq.isEmpty())
            {
                long token = pq.peek().currentToken;
                long pd = pq.peek().currentPd;

                // Write index entry only for first occurrence of each token
                if (firstEntry || token != lastIndexedToken)
                {
                    long offset = out.position();
                    idxBuf.clear();
                    idxBuf.putLong(token);
                    idxBuf.putLong(offset);
                    idxBuf.flip();
                    while (idxBuf.hasRemaining())
                        idx.write(idxBuf);
                    lastIndexedToken = token;
                    firstEntry = false;
                }

                // Gather all readers at this (token, pd); PQ orders by (token, pd, fileIndex)
                int totalCount = 0;
                ArrayList<PeekingIter> batch = new ArrayList<>();
                while (!pq.isEmpty() && pq.peek().currentToken == token && pq.peek().currentPd == pd)
                {
                    PeekingIter r = pq.poll();
                    totalCount += r.currentLtsCount;
                    batch.add(r);
                }

                // Write merged header
                headerBuf.clear();
                headerBuf.putLong(token);
                headerBuf.putLong(pd);
                headerBuf.putInt(totalCount);
                headerBuf.flip();
                while (headerBuf.hasRemaining())
                    out.write(headerBuf);

                // Transfer LTS bytes from each reader directly to output
                for (PeekingIter r : batch)
                {
                    r.appendTo(out);
                    r.advance();
                    if (r.hasMore)
                        pq.add(r);
                }
            }
        }
        finally
        {
            for (PeekingIter r : readers)
                r.close();
        }
    }
}