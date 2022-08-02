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
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.CRC32;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.apache.cassandra.service.ClientState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.service.paxos.Ballot;
import org.apache.cassandra.service.paxos.Commit;

import static org.apache.cassandra.io.util.SequentialWriterOption.FINISH_ON_CLOSE;
import static org.apache.cassandra.net.Crc.crc32;

/**
 * Tracks the highest paxos ballot we've seen, and the lowest ballot we can accept.
 *
 * During paxos repair, the coordinator gets the highest ballot seen by each participant. At the end of repair, that
 * high ballot is set as the new low bound. Combined with paxos repair during topology changes, this eliminates the
 * possibility of new nodes accepting ballots that are before the most recently accepted ballot for a key.
 */
public class PaxosBallotTracker
{
    private static final Logger logger = LoggerFactory.getLogger(PaxosBallotTracker.class);

    private static final int FILE_VERSION = 0;
    static final String FNAME = "ballot.meta";
    private static final String TMP_FNAME = FNAME + ".tmp";

    private final File directory;
    private final AtomicReference<Ballot> highBound;
    private volatile Ballot lowBound;

    private PaxosBallotTracker(File directory, Ballot highBound, Ballot lowBound)
    {
        Preconditions.checkNotNull(lowBound);
        Preconditions.checkNotNull(highBound);
        this.directory = directory;
        this.highBound = new AtomicReference<>(highBound);
        this.lowBound = lowBound;
    }

    private static void serializeBallot(SequentialWriter writer, CRC32 crc, Ballot ballot) throws IOException
    {
        ByteBuffer bytes = ballot.toBytes();
        writer.write(bytes);
        crc.update(bytes);
    }

    private static Ballot deserializeBallot(RandomAccessReader reader, CRC32 crc, byte[] bytes) throws IOException
    {
        reader.readFully(bytes);
        crc.update(bytes);
        return Ballot.deserialize(ByteBuffer.wrap(bytes));
    }

    public static void truncate(File directory) throws IOException
    {
        logger.info("truncating paxos ballot metadata in {}", directory);
        deleteIfExists(new File(directory, TMP_FNAME));
        deleteIfExists(new File(directory, FNAME));
    }

    public static PaxosBallotTracker load(File directory) throws IOException
    {
        deleteIfExists(new File(directory, TMP_FNAME));

        File file = new File(directory, FNAME);
        if (!file.exists())
            return new PaxosBallotTracker(directory, Ballot.none(), Ballot.none());

        try (RandomAccessReader reader = RandomAccessReader.open(file))
        {
            int version = reader.readInt();
            if (version != FILE_VERSION)
                throw new IOException("Unsupported ballot file version: " + version);

            byte[] bytes = new byte[16];
            CRC32 crc = crc32();
            Ballot highBallot = deserializeBallot(reader, crc, bytes);
            Ballot lowBallot = deserializeBallot(reader, crc, bytes);
            int checksum = Integer.reverseBytes(reader.readInt());
            if (!reader.isEOF() || (int) crc.getValue() != checksum)
                throw new IOException("Ballot file corrupted");

            return new PaxosBallotTracker(directory, highBallot, lowBallot);
        }
    }

    private static void deleteIfExists(File file)
    {
        if (file.exists())
            file.delete();
    }

    public synchronized void flush() throws IOException
    {
        File file = new File(directory, TMP_FNAME);
        deleteIfExists(file);

        try(SequentialWriter writer = new SequentialWriter(file, FINISH_ON_CLOSE))
        {
            CRC32 crc = crc32();
            writer.writeInt(FILE_VERSION);
            serializeBallot(writer, crc, getHighBound());
            serializeBallot(writer, crc, getLowBound());
            writer.writeInt(Integer.reverseBytes((int) crc.getValue()));
        }
        file.move(new File(directory, FNAME));
    }

    public synchronized void truncate()
    {
        deleteIfExists(new File(directory, TMP_FNAME));
        deleteIfExists(new File(directory, FNAME));
        highBound.set(Ballot.none());
        lowBound = Ballot.none();
    }

    private void updateHighBound(Ballot current, Ballot next)
    {
        while (Commit.isAfter(next, current) && !highBound.compareAndSet(current, next))
            current = highBound.get();
    }

    void updateHighBound(Ballot next)
    {
        updateHighBound(highBound.get(), next);
    }

    public void onUpdate(Row row)
    {
        Ballot current = highBound.get();
        Ballot next = PaxosRows.getHighBallot(row, current);
        if (current == next)
            return;

        updateHighBound(current, next);
    }

    @VisibleForTesting
    void updateHighBoundUnsafe(Ballot expected, Ballot update)
    {
        highBound.compareAndSet(expected, update);
    }

    public File getDirectory()
    {
        return directory;
    }

    public synchronized void updateLowBound(Ballot update) throws IOException
    {
        if (!Commit.isAfter(update, lowBound))
        {
            logger.debug("Not updating lower bound with earlier or equal ballot from {} to {}", lowBound, update);
            return;
        }

        logger.debug("Updating lower bound from {} to {}", lowBound, update);
        ClientState.getTimestampForPaxos(lowBound.unixMicros());
        lowBound = update;
        flush();
    }

    public Ballot getHighBound()
    {
        return highBound.get();
    }

    /**
     * @return a unique ballot that has never been proposed, below which we will reject all proposals
     */
    public Ballot getLowBound()
    {
        return lowBound;
    }
}
