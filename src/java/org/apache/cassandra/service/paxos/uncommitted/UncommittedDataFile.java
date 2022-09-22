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

package org.apache.cassandra.service.paxos.uncommitted;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.PeekingIterator;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.util.ChecksummedRandomAccessReader;
import org.apache.cassandra.io.util.ChecksummedSequentialWriter;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.paxos.Ballot;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.Throwables;

public class UncommittedDataFile
{
    static final String EXTENSION = "paxos";
    static final String TMP_SUFFIX = ".tmp";
    private static final int VERSION = 0;
    final TableId tableId;
    private final File file;
    private final File crcFile;
    private final long generation;
    private int activeReaders = 0;
    private boolean markedDeleted = false;

    private UncommittedDataFile(TableId tableId, File file, File crcFile, long generation)
    {
        this.tableId = tableId;
        this.file = file;
        this.crcFile = crcFile;
        this.generation = generation;
    }

    public static UncommittedDataFile create(TableId tableId, File file, File crcFile, long generation)
    {
        return new UncommittedDataFile(tableId, file, crcFile, generation);
    }

    static Writer writer(File directory, String keyspace, String table, TableId tableId, long generation) throws IOException
    {
        return new Writer(directory, keyspace, table, tableId, generation);
    }

    static Set<TableId> listTableIds(File directory)
    {
        Pattern pattern = Pattern.compile(".*-([a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12})-(\\d+)\\." + EXTENSION + '$');
        Set<TableId> tableIds = new HashSet<>();
        for (String fname : directory.listNamesUnchecked())
        {
            Matcher matcher = pattern.matcher(fname);
            if (matcher.matches())
                tableIds.add(TableId.fromUUID(UUID.fromString(matcher.group(1))));
        }
        return tableIds;
    }

    static Pattern fileRegexFor(TableId tableId)
    {
        return Pattern.compile(".*-" + tableId.toString() + "-(\\d+)\\." + EXTENSION + ".*");
    }

    static boolean isTmpFile(String fname)
    {
        return fname.endsWith(TMP_SUFFIX);
    }

    static boolean isCrcFile(String fname)
    {
        return fname.endsWith(".crc");
    }

    static String fileName(String keyspace, String table, TableId tableId, long generation)
    {
        return String.format("%s-%s-%s-%s.%s", keyspace, table, tableId, generation, EXTENSION);
    }

    static String crcName(String fname)
    {
        return fname + ".crc";
    }

    synchronized void markDeleted()
    {
        markedDeleted = true;
        maybeDelete();
    }

    private void maybeDelete()
    {
        if (markedDeleted && activeReaders == 0)
        {
            file.delete();
            crcFile.delete();
        }
    }

    synchronized private void onIteratorClose()
    {
        activeReaders--;
        maybeDelete();
    }

    @VisibleForTesting
    File file()
    {
        return file;
    }

    @VisibleForTesting
    int getActiveReaders()
    {
        return activeReaders;
    }

    @VisibleForTesting
    boolean isMarkedDeleted()
    {
        return markedDeleted;
    }

    long generation()
    {
        return generation;
    }

    /**
     * Return an iterator of the file contents for the given token ranges. Token ranges
     * must be normalized
     */
    synchronized CloseableIterator<PaxosKeyState> iterator(Collection<Range<Token>> ranges)
    {
        Preconditions.checkArgument(Iterables.elementsEqual(Range.normalize(ranges), ranges));
        if (markedDeleted)
            return null;
        activeReaders++;
        return new KeyCommitStateIterator(ranges);
    }

    private interface PeekingKeyCommitIterator extends CloseableIterator<PaxosKeyState>, PeekingIterator<PaxosKeyState>
    {
        static final PeekingKeyCommitIterator EMPTY = new PeekingKeyCommitIterator()
        {
            public PaxosKeyState peek() { throw new NoSuchElementException(); }
            public void remove() { throw new NoSuchElementException(); }
            public void close() { }
            public boolean hasNext() { return false; }
            public PaxosKeyState next() { throw new NoSuchElementException(); }
        };
    }

    static class Writer
    {
        final File directory;
        final String keyspace;
        final String table;
        final TableId tableId;
        long generation;

        private final File file;
        private final File crcFile;
        final SequentialWriter writer;
        DecoratedKey lastKey = null;

        private String fileName(long generation)
        {
            return UncommittedDataFile.fileName(keyspace, table, tableId, generation);
        }

        private String crcName(long generation)
        {
            return UncommittedDataFile.crcName(fileName(generation));
        }

        Writer(File directory, String keyspace, String table, TableId tableId, long generation) throws IOException
        {
            this.directory = directory;
            this.keyspace = keyspace;
            this.table = table;
            this.tableId = tableId;
            this.generation = generation;

            directory.createDirectoriesIfNotExists();

            this.file = new File(this.directory, fileName(generation) + TMP_SUFFIX);
            this.crcFile = new File(this.directory, crcName(generation) + TMP_SUFFIX);
            this.writer = new ChecksummedSequentialWriter(file, crcFile, null, SequentialWriterOption.DEFAULT);
            this.writer.writeInt(VERSION);
        }

        void append(PaxosKeyState state) throws IOException
        {
            if (lastKey != null)
                Preconditions.checkArgument(state.key.compareTo(lastKey) > 0);
            lastKey = state.key;
            ByteBufferUtil.writeWithShortLength(state.key.getKey(), writer);
            state.ballot.serialize(writer);
            writer.writeBoolean(state.committed);
        }

        Throwable abort(Throwable accumulate)
        {
            return writer.abort(accumulate);
        }

        UncommittedDataFile finish()
        {
            writer.finish();
            File finalCrc = new File(directory, crcName(generation));
            File finalData = new File(directory, fileName(generation));
            try
            {
                crcFile.move(finalCrc);
                file.move(finalData);
                return new UncommittedDataFile(tableId, finalData, finalCrc, generation);
            }
            catch (Throwable e)
            {
                Throwable merged = e;
                for (File f : new File[]{crcFile, finalCrc, file, finalData})
                {
                    try
                    {
                        if (f.exists())
                            Files.delete(f.toPath());
                    }
                    catch (Throwable t)
                    {
                        merged = Throwables.merge(merged, t);
                    }
                }

                if (merged != e)
                    throw new RuntimeException(merged);
                throw e;
            }
        }
    }

    class KeyCommitStateIterator extends AbstractIterator<PaxosKeyState> implements PeekingKeyCommitIterator
    {
        private final Iterator<Range<Token>> rangeIterator;
        private final RandomAccessReader reader;

        private Range<PartitionPosition> currentRange;

        KeyCommitStateIterator(Collection<Range<Token>> ranges)
        {
            this.rangeIterator = ranges.iterator();
            try
            {
                this.reader = ChecksummedRandomAccessReader.open(file, crcFile);
            }
            catch (IOException e)
            {
                throw new FSReadError(e, file);
            }
            validateVersion(this.reader);

            Preconditions.checkArgument(rangeIterator.hasNext());
            currentRange = convertRange(rangeIterator.next());
        }

        private Range<PartitionPosition> convertRange(Range<Token> tokenRange)
        {
            return new Range<>(tokenRange.left.maxKeyBound(), tokenRange.right.maxKeyBound());
        }

        private void validateVersion(RandomAccessReader reader)
        {
            try
            {
                int version = reader.readInt();
                Preconditions.checkArgument(version == VERSION, "unsupported file version: %s", version);
            }
            catch (IOException e)
            {
                throw new FSReadError(e, file);
            }
        }

        PaxosKeyState createKeyState(DecoratedKey key, RandomAccessReader reader) throws IOException
        {
            return new PaxosKeyState(tableId, key,
                                     Ballot.deserialize(reader),
                                     reader.readBoolean());
        }

        /**
         * skip any bytes after the key
         */
        void skipEntryRemainder(RandomAccessReader reader) throws IOException
        {
            reader.skipBytes((int) Ballot.sizeInBytes());
            reader.readBoolean();
        }

        protected synchronized PaxosKeyState computeNext()
        {
            try
            {
                nextKey:
                while (!reader.isEOF())
                {
                    DecoratedKey key = DatabaseDescriptor.getPartitioner().decorateKey(ByteBufferUtil.readWithShortLength(reader));

                    while (!currentRange.contains(key))
                    {
                        // if this falls before our current target range, just keep going
                        if (currentRange.left.compareTo(key) >= 0)
                        {
                            skipEntryRemainder(reader);
                            continue nextKey;
                        }

                        // otherwise check against subsequent ranges and end iteration if there are none
                        if (!rangeIterator.hasNext())
                            return endOfData();

                        currentRange = convertRange(rangeIterator.next());
                    }

                    return createKeyState(key, reader);
                }
                return endOfData();
            }
            catch (IOException e)
            {
                throw new FSReadError(e, file);
            }
        }

        public void close()
        {
            synchronized (this)
            {
                reader.close();
            }
            onIteratorClose();
        }
    }
}
