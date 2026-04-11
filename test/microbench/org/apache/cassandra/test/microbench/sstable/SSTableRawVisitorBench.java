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

package org.apache.cassandra.test.microbench.sstable;

import java.io.IOException;

import com.google.common.collect.ImmutableList;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.rows.UnfilteredSerializer;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.vint.VIntCoding;

@State(Scope.Benchmark)
public class SSTableRawVisitorBench extends SSTableAbstractBench
{
    private Version version;
    private TableMetadata metadata;
    private ImmutableList<ColumnMetadata> clusteringColumns;
    private int clusteringColumnCount;
    private AbstractType<?>[] clusteringColumnTypes;
    private boolean hasUIntDeletionTime;

    private SSTableReader ssTableReader;

    @Setup(Level.Invocation)
    public void prepareReader() throws IOException
    {
        ssTableReader = getReader();
        TableMetadata metadata = ssTableReader.metadata();
        version = ssTableReader.descriptor.version;
        hasUIntDeletionTime = version.hasUIntDeletionTime();
        clusteringColumns = metadata.clusteringColumns();
        clusteringColumnCount = clusteringColumns.size();
        clusteringColumnTypes = new AbstractType<?>[clusteringColumnCount];
        for (int i = 0; i< clusteringColumnTypes.length; i++) {
            clusteringColumnTypes[i] = clusteringColumns.get(i).type;
        }
    }

    @TearDown(Level.Invocation)
    public void closeReader() {
        ssTableReader.ref().close();
    }

    long[] counters = new long[4];
    @Benchmark
    public void countPartitionsAndUnfiltered() throws IOException
    {
        for (int i = 0; i < counters.length; i++)
        {
            counters[i] = 0;
        }
        try (RandomAccessReader randomAccessReader = ssTableReader.openDataReader()) {
            long length = randomAccessReader.length();
            long nextPartition = 0;
            do
            {
                nextPartition = readPartition(randomAccessReader, nextPartition, counters);
                counters[0]++;
            } while (!randomAccessReader.isEOF() && nextPartition < length);
        }
    }


    // struct partition {
    //   struct partition_header header
    //   optional<struct row> row
    //   struct unfiltered unfiltereds[];
    //};
    private long readPartition(RandomAccessReader randomAccessReader, long nextPartition, long[] counters) throws IOException
    {
        int cursor = (int) nextPartition;
        int headerPosition = cursor;
        //   struct partition_header header {
        //     be16 key_length; e.g. 8 if long
        //     byte key[key_length];
        //     struct deletion_time deletion_time {
        //       be32 local_deletion_time;
        //       be64 marked_for_delete_at;
        //     };
        //   };
        int keyLength = randomAccessReader.readUnsignedShort();
        // TODO: print key according to metadata (need the type for formatting)
        int keyPosition = (cursor += 2);
        randomAccessReader.skipBytes(keyLength);
        cursor += keyLength;

        // PARTITION DELETION TIME
        int deletionTimePosition = cursor;
        int deletionTimeSize = 12;
        if (hasUIntDeletionTime) {
            byte flags = randomAccessReader.readByte();
            if ((IS_LIVE_DELETION & flags) != 0) {
                deletionTimeSize = 1;
                // no delete times
            }
            else {
                long position = randomAccessReader.getPosition();
                randomAccessReader.seek(position - 1);
                long markedForDeleteAt = randomAccessReader.readLong();
                int localDeletionTime = randomAccessReader.readInt();
            }
        }
        else
        {
            int localDeletionTime = randomAccessReader.readInt();
            long markedForDeleteAt = randomAccessReader.readLong();
        }
        // read the rows until END_OF_PARTITION
        int nextUnfilteredPosition = (cursor += deletionTimeSize);
        byte nextUnfilteredFlags = randomAccessReader.readByte();
        while (!UnfilteredSerializer.isEndOfPartition(nextUnfilteredFlags)) {
            nextUnfilteredPosition = readUnfiltered(randomAccessReader, nextUnfilteredFlags, nextUnfilteredPosition, counters);
            nextUnfilteredFlags = randomAccessReader.readByte();
        }
        return nextUnfilteredPosition + 1;
    }
    // struct row {
    //   byte flags;
    //   optional<byte> extended_flags; // only present for static rows
    //   optional<struct clustering_block[]> clustering_blocks {
    //     varint clustering_block_header;
    //     simple_cell[] clustering_cells;
    //   }; // only present for non-static rows
    //   varint row_body_size;
    //   varint prev_unfiltered_size; // for backward traversing
    //   optional<struct liveness_info> liveness_info;
    //   optional<struct delta_deletion_time> deletion_time;
    //   optional<varint[]> missing_columns;
    //   cell[] cells;
    // }; // Has IS_STATIC flag set
    private int readUnfiltered(RandomAccessReader randomAccessReader, byte flags, final int unfilteredStartPosition, long[] counters) throws IOException
    {
        if (UnfilteredSerializer.isEndOfPartition(flags)) throw new IllegalStateException();

        int cursor = unfilteredStartPosition + 1;
        boolean isRow = UnfilteredSerializer.isRow(flags);
        boolean isTombstoneMarker = UnfilteredSerializer.isTombstoneMarker(flags);
        boolean isStatic = false;
        boolean deletionIsShadowable = false;
        if (UnfilteredSerializer.isExtended(flags)) {
            byte extendedFlags = randomAccessReader.readByte(); cursor++;

            isStatic = UnfilteredSerializer.isStatic(extendedFlags);
            deletionIsShadowable = UnfilteredSerializer.deletionIsShadowable(extendedFlags);

        }
        if ((isStatic && !isRow) || (isStatic && isTombstoneMarker)) throw new IllegalStateException();

        if (isStatic) { // this should only apply to first row read
            // static row
            long rowSize = randomAccessReader.readUnsignedVInt();
            randomAccessReader.skipBytes((int)rowSize);

            cursor += VIntCoding.computeUnsignedVIntSize(rowSize) + rowSize;
            // TODO: handle row contents

            counters[1]++;
        }
        else if (isRow)
        {
            final int rowClusteringStart = cursor;
            // READ CLUSTERING, repeated for tombstone, will de-dup later
            long clusteringBlockHeader = 0;
            AbstractType<?>[] types = clusteringColumnTypes;
            for (int clusteringIndex = 0; clusteringIndex < types.length; clusteringIndex++)
            {
                // struct clustering_block {
                //    varint clustering_block_header;
                //    simple_cell[] clustering_cells;
                // };
                if (clusteringIndex % 32 == 0) {
                    // TODO: ideally we'd like to get the size while reading rather than have to compute it
                    clusteringBlockHeader = randomAccessReader.readUnsignedVInt();
                    cursor += VIntCoding.computeUnsignedVIntSize(clusteringBlockHeader);
                }
                AbstractType<?> type = types[clusteringIndex];
                if (isNull(clusteringBlockHeader, clusteringIndex)) {
                    // handle null
                } else if (isEmpty(clusteringBlockHeader, clusteringIndex)) {
                    // handle empty
                } else if (type.isValueLengthFixed()) {
                    // handle value (TODO: add some JSON sonversion without Strings)
                    int length = type.valueLengthIfFixed();
                    cursor += length;
                    randomAccessReader.skipBytes(length);
                } else {
                    int length = randomAccessReader.readUnsignedVInt32();
                    cursor += VIntCoding.computeUnsignedVIntSize(length);
                    if (length < 0)
                        throw new IllegalStateException("Corrupt (negative) value length encountered");
                    // handle value (TODO: add some JSON sonversion without Strings)
                    cursor += length;
                    randomAccessReader.skipBytes(length);
                }
            }
            // READ CLUSTERING DONE
            final int rowBodyStart = cursor;

            long rowSize = randomAccessReader.readUnsignedVInt();
            randomAccessReader.skipBytes((int)rowSize);
            cursor += VIntCoding.computeUnsignedVIntSize(rowSize) + rowSize;
            // TODO: handle row contents
            counters[2]++;
        }
        else if (isTombstoneMarker) {
            //  struct range_tombstone_marker {
            //      byte flags = IS_MARKER;
            //      byte kind_ordinal;
            //      be16 bound_values_count;
            //      struct clustering_block[] clustering_blocks;
            //      varint marker_body_size;
            //      varint prev_unfiltered_size;
            //  };
            byte kind = randomAccessReader.readByte();
            cursor++;

            int clusteringColumnsBound = randomAccessReader.readUnsignedShort();
            cursor+=2;

            // READ CLUSTERING, repeated for row, will de-dup later
            long clusteringBlockHeader = 0;
            AbstractType<?>[] types = clusteringColumnTypes;
            for (int clusteringIndex = 0; clusteringIndex < clusteringColumnsBound; clusteringIndex++)
            {
                // struct clustering_block {
                //    varint clustering_block_header;
                //    simple_cell[] clustering_cells;
                // };
                if (clusteringIndex % 32 == 0) {
                    // TODO: ideally we'd like to get the size while reading rather than have to compute it
                    clusteringBlockHeader = randomAccessReader.readUnsignedVInt();
                    cursor += VIntCoding.computeUnsignedVIntSize(clusteringBlockHeader);
                }
                AbstractType<?> type = types[clusteringIndex];
                if (isNull(clusteringBlockHeader, clusteringIndex)) {
                    // handle null
                } else if (isEmpty(clusteringBlockHeader, clusteringIndex)) {
                    // handle empty
                } else if (type.isValueLengthFixed()) {
                    // handle value (TODO: add some JSON sonversion without Strings)
                    cursor += type.valueLengthIfFixed();
                } else {
                    int length = randomAccessReader.readUnsignedVInt32();
                    //cursor += VIntCoding.computeUnsignedVIntSize(length);
                    if (length < 0)
                        throw new IllegalStateException("Corrupt (negative) value length encountered");
                    // handle value (TODO: add some JSON sonversion without Strings)
                    cursor += length;
                }
            }
            // READ CLUSTERING DONE
            long length = randomAccessReader.readUnsignedVInt();
            cursor += VIntCoding.computeUnsignedVIntSize(length);
            length = randomAccessReader.readUnsignedVInt();
            cursor += VIntCoding.computeUnsignedVIntSize(length);
            counters[3]++;
        }

        return cursor;
    }

    // TODO: C&P from Clustering
    // ---Clustering
    // no need to do modulo arithmetic for i, since the left-shift execute on the modulus of RH operand by definition
    private static boolean isNull(long header, int i)
    {
        long mask = 1L << (i * 2) + 1;
        return (header & mask) != 0;
    }

    // no need to do modulo arithmetic for i, since the left-shift execute on the modulus of RH operand by definition
    private static boolean isEmpty(long header, int i)
    {
        long mask = 1L << (i * 2);
        return (header & mask) != 0;
    }
    // ---Clustering

    // TODO: C&P from DeletionTime
    // We use the sign bit to signal LIVE DeletionTimes
    private final static int IS_LIVE_DELETION = 0b1000_0000;
}
