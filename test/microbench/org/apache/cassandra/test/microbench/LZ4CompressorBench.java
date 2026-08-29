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

package org.apache.cassandra.test.microbench;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Comparator;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import org.apache.cassandra.io.compress.LZ4Compressor;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;

/**
 * This benchmark measures the sstable compression path of {@link LZ4Compressor}. It calls the
 * {@code compress} and {@code uncompress} methods that take {@link ByteBuffer} arguments, and it gives
 * these methods off-heap buffers. {@link org.apache.cassandra.io.compress.CompressedSequentialWriter}
 * and {@link org.apache.cassandra.io.util.CompressedChunkReader} use the same two methods.
 * <p>
 * The benchmark makes the compressor from empty options. Thus the compressor is the {@code fast}
 * compressor, which is the compressor that sstables use by default.
 * <p>
 * The default chunk size is {@link org.apache.cassandra.schema.CompressionParams#DEFAULT_CHUNK_LENGTH},
 * which is 16 KiB. Sstables use this size, unless {@code chunk_length_in_kb} gives a different size. To
 * measure other sizes, use {@code -Djmh.args="-p chunkLength=4096,65536"}.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1, jvmArgsAppend = { "-Dcassandra.schema.force_load_local_keyspaces=true" })
@Threads(1)
@State(Scope.Thread)
public class LZ4CompressorBench
{
    @Param({ "16384" })
    private int chunkLength;

    private LZ4Compressor compressor;

    private ByteBuffer uncompressed;
    private ByteBuffer compressed;
    private ByteBuffer compressedInput;
    private int compressedLength;
    private ByteBuffer decompressed;

    @Setup(Level.Trial)
    public void setup() throws IOException
    {
        // Empty options select the fast compressor.
        // A table with 'class': 'LZ4Compressor' gets the same compressor.
        compressor = LZ4Compressor.create(Collections.emptyMap());

        uncompressed = ByteBuffer.allocateDirect(chunkLength);
        uncompressed.put(sstablePartitions(chunkLength, new Random(1)));
        uncompressed.flip();

        int maxCompressedLength = compressor.initialCompressedBufferLength(chunkLength);
        compressed = ByteBuffer.allocateDirect(maxCompressedLength);
        compressedInput = ByteBuffer.allocateDirect(maxCompressedLength);
        decompressed = ByteBuffer.allocateDirect(chunkLength);

        compressor.compress(uncompressed.duplicate(), compressedInput);
        compressedLength = compressedInput.position();
        compressedInput.flip();

        // Stop immediately if the compressor cannot complete a round trip. A library upgrade can cause
        // this fault.
        compressor.uncompress(compressedInput.duplicate(), decompressed);
        if (decompressed.position() != chunkLength)
            throw new IllegalStateException("Round-trip produced " + decompressed.position() + " bytes, expected " + chunkLength);

        System.out.println(String.format("sstable chunk: %d -> %d bytes, ratio %.3f",
                                         chunkLength, compressedLength, compressedLength / (double) chunkLength));
    }

    @Benchmark
    public ByteBuffer compress() throws IOException
    {
        uncompressed.clear();
        compressed.clear();
        compressor.compress(uncompressed, compressed);
        return compressed;
    }

    @Benchmark
    public ByteBuffer uncompress() throws IOException
    {
        compressedInput.clear();
        compressedInput.limit(compressedLength);
        decompressed.clear();
        compressor.uncompress(compressedInput, decompressed);
        return decompressed;
    }

    private static final int ROWS_PER_PARTITION = 10;
    private static final int VALUE_COLUMNS = 10;
    private static final int TEXT_LENGTH = 10;

    /**
     * The number of different strings that the value columns use.
     * <p>
     * A small set of different values makes a real sstable compressible. Examples are statuses, types
     * and tags. This constant sets the compression ratio. If each cell gets a different random value,
     * the ratio is approximately 0.86. At that ratio the data is almost incompressible, and
     * decompression copies literal bytes only. A value of 96 gives a ratio of approximately 0.30 for a
     * 16 KiB chunk, which is usual for a compressible table.
     * <p>
     * These are the measured ratios at {@code chunkLength=16384}:
     * <pre>
     *   16 -&gt; 0.251    48 -&gt; 0.272    192 -&gt; 0.346    768 -&gt; 0.555
     *   32 -&gt; 0.260    96 -&gt; 0.298    384 -&gt; 0.435
     * </pre>
     * The minimum ratio is approximately 0.25, not 0. The other bytes in the chunk are sstable
     * structure: partition headers, clustering prefixes, timestamp deltas and row flags. This structure
     * stays repetitive.
     */
    private static final int VALUE_CARDINALITY = 96;

    /**
     * This method writes an sstable with {@link CQLSSTableWriter}. Then it returns the first
     * {@code size} bytes of the {@code Data.db} file. The sstable has no compression. Therefore these
     * bytes are the same bytes that
     * {@link org.apache.cassandra.io.compress.CompressedSequentialWriter} gives to the compressor as one
     * chunk. The bytes contain partition headers, row flags, clustering prefixes, timestamp deltas and
     * cell data, in token order.
     * <p>
     * The table has one text partition key and one text clustering key. Each key is a sequence of
     * {@link #TEXT_LENGTH} digits with zeros at the start. The table also has {@link #VALUE_COLUMNS}
     * text columns. Each column holds one of {@link #VALUE_CARDINALITY} different strings, and each
     * string has {@link #TEXT_LENGTH} characters. The method writes {@link #ROWS_PER_PARTITION} rows in
     * each partition.
     */
    private static byte[] sstablePartitions(int size, Random random) throws IOException
    {
        StringBuilder schema = new StringBuilder("CREATE TABLE lz4_bench.partitions (pk text, ck text");
        StringBuilder names = new StringBuilder("pk, ck");
        StringBuilder binds = new StringBuilder("?, ?");
        for (int c = 0; c < VALUE_COLUMNS; c++)
        {
            schema.append(", v").append(c).append(" text");
            names.append(", v").append(c);
            binds.append(", ?");
        }
        schema.append(", PRIMARY KEY (pk, ck)) WITH compression = {'enabled': 'false'}");
        // Use a constant timestamp. A different timestamp changes the delta-encoded row liveness bytes on each run.
        String insert = "INSERT INTO lz4_bench.partitions (" + names + ") VALUES (" + binds + ") USING TIMESTAMP 1755000000000000";

        // One partition is approximately 1.4 KiB. Thus the sstable holds more bytes than the chunk.
        int partitions = Math.max(16, size / 256);

        Path dir = Files.createTempDirectory("lz4-compressor-bench");
        try
        {
            try (CQLSSTableWriter writer = CQLSSTableWriter.builder()
                                                           .inDirectory(dir.toString())
                                                           .forTable(schema.toString())
                                                           .using(insert)
                                                           .build())
            {
                String[] dictionary = new String[VALUE_CARDINALITY];
                for (int i = 0; i < dictionary.length; i++)
                    dictionary[i] = randomAscii(random);

                Object[] values = new Object[2 + VALUE_COLUMNS];
                for (int p = 0; p < partitions; p++)
                {
                    values[0] = paddedSequence(p);
                    for (int r = 0; r < ROWS_PER_PARTITION; r++)
                    {
                        values[1] = paddedSequence(r);
                        for (int c = 0; c < VALUE_COLUMNS; c++)
                            values[2 + c] = dictionary[random.nextInt(dictionary.length)];
                        writer.addRow(values);
                    }
                }
            }

            Path data;
            try (Stream<Path> written = Files.list(dir))
            {
                data = written.filter(p -> p.getFileName().toString().endsWith("-Data.db"))
                              .findFirst()
                              .orElseThrow(() -> new IOException("CQLSSTableWriter produced no Data.db in " + dir));
            }

            byte[] chunk = new byte[size];
            try (InputStream in = Files.newInputStream(data))
            {
                int read = in.readNBytes(chunk, 0, size);
                if (read != size)
                    throw new IOException("Only " + read + " of " + size + " bytes of sstable data available, "
                                          + "raise the partition count");
            }
            return chunk;
        }
        finally
        {
            deleteRecursive(dir);
        }
    }

    private static String paddedSequence(int sequence)
    {
        return String.format("%0" + TEXT_LENGTH + "d", sequence);
    }

    private static String randomAscii(Random random)
    {
        char[] text = new char[TEXT_LENGTH];
        for (int i = 0; i < text.length; i++)
            text[i] = (char) ('!' + random.nextInt('~' - '!' + 1));
        return new String(text);
    }

    private static void deleteRecursive(Path dir) throws IOException
    {
        try (Stream<Path> paths = Files.walk(dir))
        {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toArray(Path[]::new))
                Files.deleteIfExists(path);
        }
    }
}
