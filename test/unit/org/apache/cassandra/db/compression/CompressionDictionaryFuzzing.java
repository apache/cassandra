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

package org.apache.cassandra.db.compression;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeSet;

import javax.annotation.Nullable;

import com.github.benmanes.caffeine.cache.Cache;

import accord.utils.RandomSource;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compression.CompressionDictionary.DictId;
import org.apache.cassandra.io.compress.ZstdDictionaryCompressor;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.utils.concurrent.Ref;

import static java.lang.String.format;

/**
 * Data, table configurations and access to the dictionary caches shared by the fuzz tests of compression dictionaries
 * on one node and on a cluster.
 */
public final class CompressionDictionaryFuzzing
{
    public static final int PARTITIONS = 300;
    public static final int ROWS_PER_PARTITION = 40;

    // A training samples at least this many chunks, so that it collects more than the 11 samples a training needs
    // also when its sstables have little data each and contribute fewer chunks than their share.
    public static final int MIN_SAMPLED_CHUNKS = 32;
    // Before a training, a table gets at least this many times the maximum sample size of data.
    public static final double DATA_PER_SAMPLE_SIZE = 1.5;
    public static final int TOP_UP_VALUE_LENGTH = 2048;

    private static final String[] LEVELS = { "TRACE", "DEBUG", "INFO", "WARN", "ERROR" };
    private static final String[] SERVICES = { "UserService", "PaymentService", "OrderService", "InventoryService", "SearchService" };
    private static final String[] ACTIONS = { "login", "logout", "checkout", "refund", "search", "upload", "download" };

    private CompressionDictionaryFuzzing()
    {
    }

    public static class WriteRow
    {
        public final int pk;
        public final int ck;
        public final String value;

        public WriteRow(int pk, int ck, String value)
        {
            this.pk = pk;
            this.ck = ck;
            this.value = value;
        }
    }

    public static class Compression
    {
        static final String DICTIONARY_COMPRESSOR = "ZstdDictionaryCompressor";

        @Nullable
        public final String compressor;
        public final int chunkLengthInKiB;
        public final int level;
        public final int maxDictionarySizeInKiB;
        public final int maxTotalSampleSizeInKiB;

        Compression(String compressor, int chunkLengthInKiB, int level, int maxDictionarySizeInKiB, int maxTotalSampleSizeInKiB)
        {
            this.compressor = compressor;
            this.chunkLengthInKiB = chunkLengthInKiB;
            this.level = level;
            this.maxDictionarySizeInKiB = maxDictionarySizeInKiB;
            this.maxTotalSampleSizeInKiB = maxTotalSampleSizeInKiB;
        }

        public static Compression random(RandomSource rs)
        {
            int chunkLengthInKiB = rs.pick(4, 8, 16, 64);
            int level = rs.nextInt(-3, 20);
            switch (rs.nextInt(10))
            {
                case 0:
                    return new Compression(null, chunkLengthInKiB, level, 0, 0);
                case 1:
                    return new Compression("LZ4Compressor", chunkLengthInKiB, level, 0, 0);
                case 2:
                    return new Compression("ZstdCompressor", chunkLengthInKiB, level, 0, 0);
                default:
                    int maxTotalSampleSizeInKiB = maxTotalSampleSizeInKiB(rs, chunkLengthInKiB);
                    return new Compression(DICTIONARY_COMPRESSOR, chunkLengthInKiB, level,
                                           maxDictionarySizeInKiB(rs, maxTotalSampleSizeInKiB), maxTotalSampleSizeInKiB);
            }
        }

        public static int maxTotalSampleSizeInKiB(RandomSource rs, int chunkLengthInKiB)
        {
            int minimum = MIN_SAMPLED_CHUNKS * chunkLengthInKiB;
            return rs.pick(minimum, 2 * minimum, Math.max(minimum, 4096));
        }

        // much smaller than the samples, which Zstd needs to train a dictionary
        public static int maxDictionarySizeInKiB(RandomSource rs, int maxTotalSampleSizeInKiB)
        {
            return Math.min(rs.pick(4, 16, 64), maxTotalSampleSizeInKiB / 16);
        }

        public boolean hasDictionary()
        {
            return DICTIONARY_COMPRESSOR.equals(compressor);
        }

        public Compression withLevel(int level)
        {
            return new Compression(compressor, chunkLengthInKiB, level, maxDictionarySizeInKiB, maxTotalSampleSizeInKiB);
        }

        @Override
        public String toString()
        {
            if (compressor == null)
                return "{'enabled': 'false'}";

            StringBuilder cql = new StringBuilder(format("{'class': '%s', 'chunk_length_in_kb': '%d'", compressor, chunkLengthInKiB));
            if (!compressor.equals("LZ4Compressor"))
                cql.append(format(", 'compression_level': '%d'", level));
            if (hasDictionary())
                cql.append(format(", 'training_min_frequency': '0m', 'training_max_dictionary_size': '%dKiB', 'training_max_total_sample_size': '%dKiB'",
                                  maxDictionarySizeInKiB, maxTotalSampleSizeInKiB));
            return cql.append('}').toString();
        }
    }

    public static class TableSpec
    {
        public final String name;
        public final Compression compression;
        public final String compaction;
        public final boolean autoCompaction;
        // configuration of the dictionary cache of the table
        public final int cacheSize;
        public final String cacheExpire;

        TableSpec(String name, Compression compression, String compaction, boolean autoCompaction, int cacheSize, String cacheExpire)
        {
            this.name = name;
            this.compression = compression;
            this.compaction = compaction;
            this.autoCompaction = autoCompaction;
            this.cacheSize = cacheSize;
            this.cacheExpire = cacheExpire;
        }

        public static TableSpec random(RandomSource rs, String name, Integer[] cacheSizes, String[] cacheExpires)
        {
            return new TableSpec(name,
                                 Compression.random(rs),
                                 rs.pick("{'class': 'SizeTieredCompactionStrategy', 'min_threshold': '2'}",
                                         "{'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': '1'}",
                                         "{'class': 'UnifiedCompactionStrategy'}"),
                                 rs.decide(0.7f),
                                 rs.pick(cacheSizes),
                                 rs.pick(cacheExpires));
        }

        public String createStatement(String keyspace)
        {
            return format("CREATE TABLE %s.%s (pk int, ck int, v text, PRIMARY KEY (pk, ck)) WITH compression = %s AND compaction = %s",
                          keyspace, name, compression, compaction);
        }

        @Override
        public String toString()
        {
            return format("%s (compression %s, compaction %s%s, dictionary cache of size %d expiring after %s)",
                          name, compression, compaction, autoCompaction ? "" : " disabled", cacheSize, cacheExpire);
        }
    }

    /**
     * @return the key of the row in a model of a table
     */
    public static long key(int pk, int ck)
    {
        return ((long) pk << 32) | ck;
    }

    /**
     * @return the key of a partition in the rows most of the time, or of any partition
     */
    public static int partitionKey(RandomSource rs, NavigableMap<Long, String> rows)
    {
        int pk = rs.nextInt(PARTITIONS);
        if (rows.isEmpty() || rs.decide(0.2f))
            return pk;
        Long key = rows.ceilingKey(key(pk, 0));
        return (int) ((key == null ? rows.firstKey() : key) >>> 32);
    }

    public static List<WriteRow> randomRows(RandomSource rs, int count)
    {
        List<WriteRow> rows = new ArrayList<>(count);
        for (int i = 0; i < count; i++)
            rows.add(new WriteRow(rs.nextInt(PARTITIONS), rs.nextInt(ROWS_PER_PARTITION), value(rs)));
        return rows;
    }

    /**
     * @return rows with big values, together of at least the given size
     */
    public static List<WriteRow> topUpRows(RandomSource rs, long size)
    {
        List<WriteRow> rows = new ArrayList<>();
        for (long written = 0; written < size; written += TOP_UP_VALUE_LENGTH)
        {
            StringBuilder value = new StringBuilder(value(rs));
            while (value.length() < TOP_UP_VALUE_LENGTH)
                value.append(value(rs));
            rows.add(new WriteRow(rs.nextInt(PARTITIONS), rs.nextInt(ROWS_PER_PARTITION), value.toString()));
        }
        return rows;
    }

    // compressible like log lines, so that dictionaries can be trained from it
    public static String value(RandomSource rs)
    {
        StringBuilder value = new StringBuilder();
        for (int i = rs.nextInt(1, 4); i > 0; i--)
            value.append(format("2025-10-%02d %02d:%02d:%02d.%03d %-5s [com.example.%s] user=%d action=%s session=%08x took=%dms; ",
                                rs.nextInt(1, 29), rs.nextInt(24), rs.nextInt(60), rs.nextInt(60), rs.nextInt(1000),
                                rs.pick(LEVELS), rs.pick(SERVICES), rs.nextInt(10000), rs.pick(ACTIONS), rs.nextInt(), rs.nextInt(2000)));
        return value.toString();
    }

    /**
     * @return null when the rows are equal, otherwise how they differ
     */
    @Nullable
    public static String difference(NavigableMap<Long, String> expected, NavigableMap<Long, String> actual)
    {
        if (expected.equals(actual))
            return null;

        List<String> differences = new ArrayList<>();
        TreeSet<Long> keys = new TreeSet<>(expected.keySet());
        keys.addAll(actual.keySet());
        for (long key : keys)
        {
            String expectedValue = expected.get(key);
            String actualValue = actual.get(key);
            if (!Objects.equals(expectedValue, actualValue))
                differences.add(format("%d:%d expected %s, actual %s", key >>> 32, (int) key,
                                       expectedValue == null ? "no row" : '"' + expectedValue + '"',
                                       actualValue == null ? "no row" : '"' + actualValue + '"'));
        }
        return format("%d wrong rows, e.g. %s", differences.size(), differences.subList(0, Math.min(5, differences.size())));
    }

    public static boolean messageStartsWith(Throwable throwable, String prefix)
    {
        String message = throwable.getMessage();
        return message != null && message.startsWith(prefix);
    }

    /**
     * @return whether an import was rejected because the target has a newer dictionary, or already has this one, e.g.
     *         imported from the same source before
     */
    public static boolean isNotNewer(IllegalArgumentException e)
    {
        return messageStartsWith(e, "Dictionary to import has older dictionary id")
               || (messageStartsWith(e, "Dictionary to import (") && e.getMessage().contains(") is already stored for table "));
    }

    /**
     * Copies the files of an sstable into a directory, e.g. to import them later.
     */
    public static void copy(SSTableReader sstable, Path directory) throws IOException
    {
        for (Component component : sstable.descriptor.discoverComponents())
        {
            File file = sstable.descriptor.fileFor(component);
            Files.copy(file.toPath(), directory.resolve(file.name()));
        }
    }

    public static boolean isReleased(CompressionDictionary dictionary)
    {
        Ref<? extends CompressionDictionary> ref = dictionary.selfRef();
        return ref != null && ref.globalCount() <= 0;
    }

    @SuppressWarnings("unchecked")
    public static Cache<ZstdCompressionDictionary, ZstdDictionaryCompressor> compressorCache()
    {
        return (Cache<ZstdCompressionDictionary, ZstdDictionaryCompressor>) field(ZstdDictionaryCompressor.class, null, "instancePerDict");
    }

    public static ZstdCompressionDictionary dictionaryOf(ZstdDictionaryCompressor compressor)
    {
        return (ZstdCompressionDictionary) field(ZstdDictionaryCompressor.class, compressor, "dictionary");
    }

    @SuppressWarnings("unchecked")
    public static Cache<DictId, CompressionDictionary> dictionaryCache(ColumnFamilyStore cfs)
    {
        Object cache = field(CompressionDictionaryManager.class, cfs.compressionDictionaryManager(), "cache");
        return (Cache<DictId, CompressionDictionary>) field(CompressionDictionaryCache.class, cache, "cache");
    }

    /**
     * @param how 0 invalidates the whole cache, 1 one entry of it, chosen by the index, 2 cleans it up, e.g. drops
     *            what expired
     */
    public static <K> void evict(Cache<K, ?> cache, int how, int index)
    {
        switch (how)
        {
            case 0:
                cache.invalidateAll();
                break;
            case 1:
                List<K> keys = new ArrayList<>(cache.asMap().keySet());
                if (!keys.isEmpty())
                    cache.invalidate(keys.get(index % keys.size()));
                break;
            default:
                cache.cleanUp();
        }
    }

    public static String describeEviction(int how, String cache)
    {
        return format("%s %s", how == 0 ? "Invalidate" : how == 1 ? "Evict one entry from" : "Clean up", cache);
    }

    /**
     * Checks that the cache holds no released dictionary.
     *
     * @return null, or what is wrong
     */
    @Nullable
    public static String checkCached(Cache<DictId, CompressionDictionary> cache, String what, List<CompressionDictionary> seen)
    {
        List<String> problems = new ArrayList<>();
        cache.asMap().forEach((dictId, dictionary) -> {
            seen.add(dictionary);
            // the cache drops an entry before the removal listener releases its dictionary
            if (isReleased(dictionary) && cache.asMap().get(dictId) == dictionary)
                problems.add(format("%s holds the released dictionary %s", what, dictId));
        });
        return problems.isEmpty() ? null : String.join("; ", problems);
    }

    /**
     * Checks that the compressor cache holds no compressor of a released dictionary.
     *
     * @return null, or what is wrong
     */
    @Nullable
    public static String checkCompressors(List<CompressionDictionary> seen)
    {
        Cache<ZstdCompressionDictionary, ZstdDictionaryCompressor> compressors = compressorCache();
        List<String> problems = new ArrayList<>();
        compressors.asMap().forEach((key, compressor) -> {
            ZstdCompressionDictionary dictionary = dictionaryOf(compressor);
            if (dictionary == null)
                return;
            seen.add(dictionary);
            // the cache drops an entry before the removal listener releases its dictionary
            if (isReleased(dictionary) && compressors.asMap().get(key) == compressor)
                problems.add(format("the compressor cache holds a compressor of the released dictionary %s", dictionary.dictId()));
        });
        return problems.isEmpty() ? null : String.join("; ", problems);
    }

    private static Object field(Class<?> klass, Object instance, String name)
    {
        try
        {
            Field field = klass.getDeclaredField(name);
            field.setAccessible(true);
            return field.get(instance);
        }
        catch (ReflectiveOperationException e)
        {
            throw new AssertionError(e);
        }
    }
}
