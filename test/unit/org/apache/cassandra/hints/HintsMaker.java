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

package org.apache.cassandra.hints;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.UpdateBuilder;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.memory.MemoryUtil;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "hintsmaker",
         description = "Generate hints file",
         subcommands = { CommandLine.HelpCommand.class, HintsMaker.MakeHint.class })
public class HintsMaker implements Runnable
{
    public static void main(String[] args)
    {
        System.exit(new HintsMaker().execute(args));
    }

    public int execute(String[] args)
    {
        CommandLine commandLine = new CommandLine(HintsMaker.class);
        commandLine.setExecutionExceptionHandler((ex, c, arg) -> {
            ex.printStackTrace(System.err);
            return 1;
        });
        return commandLine.execute(args);
    }

    @Override
    public void run()
    {
        CommandLine.usage(this, System.out);
    }

    @Command(name = "make", description = "make file of hints")
    public static class MakeHint implements Runnable
    {
        private static final String KEYSPACE = "Keyspace1";
        private static final String TABLE = "Standard1";
        private static final String CELLNAME = "name";

        private static final String DATA_DIR = "test/data/legacy-hints/";
        private static final String PROPERTIES_FILE = "hash.txt";
        private static final String HOST_ID_PROPERTY = "hostId";
        private static final String CFID_PROPERTY = "cfid";
        private static final String CELLS_PROPERTY = "cells";
        private static final String DESCRIPTOR_TIMESTAMP_PROPERTY = "descriptorTimestamp";
        private static final String HASH_PROPERTY = "hash";

        private static ByteBuffer dataSource;

        @Option(names = "dir")
        private String dir = DATA_DIR;

        @Option(names = HOST_ID_PROPERTY)
        private UUID hostId = UUID.randomUUID();

        @Option(names = "maxLength") // 1MB by default
        private long maxLength = 1024 * 1024;

        @Option(names = "randomSize")
        private boolean randomSize;

        @Option(names = "cellSize")
        private int cellSize = 256;

        @Option(names = "numCells")
        private int numCells = 1;

        @Option(names = DESCRIPTOR_TIMESTAMP_PROPERTY)
        private long descriptorTimestamp = System.currentTimeMillis();

        public void run()
        {
            try
            {
                initialize();

                maxLength = maxLength == 0 ? DatabaseDescriptor.getMaxHintsFileSize() : maxLength;
                File dataDir = new File(dir + FBUtilities.getReleaseVersionString());

                Files.createDirectories(dataDir.toPath());

                System.out.printf("Going to generate hints file into directory %s with max length %s, host id %s and " +
                                  "hints descriptor timestamp %s", dataDir, maxLength, hostId, descriptorTimestamp);

                HintsDescriptor hintsDescriptor = new HintsDescriptor(hostId, descriptorTimestamp);

                final TableMetadata tableMetadata = Schema.instance.getTableMetadata(KEYSPACE, TABLE);

                final AtomicLong counter = new AtomicLong(0);
                final AtomicInteger hash = new AtomicInteger(0);
                final AtomicInteger cells = new AtomicInteger(0);
                final int numCells = this.numCells;
                final AtomicInteger dataSize = new AtomicInteger(0);

                Iterator<Mutation> mutationIterator = Stream.generate(() -> {
                    ThreadLocalRandom current = ThreadLocalRandom.current();
                    ByteBuffer key = randomBytes(16, current);
                    UpdateBuilder builder = UpdateBuilder.create(tableMetadata, Util.dk(key));

                    for (int i = 0; i < numCells; i++)
                    {
                        int sz = randomSize ? current.nextInt(cellSize) : cellSize;
                        ByteBuffer bytes = randomBytes(sz, current);
                        builder.newRow(CELLNAME + i).add("val", bytes);
                        hash.set(hash(hash.get(), bytes));
                        cells.incrementAndGet();
                        dataSize.addAndGet(sz);
                    }

                    counter.incrementAndGet();

                    return (Mutation) builder.makeMutation();
                }).iterator();

                makeHintFile(dataDir, hintsDescriptor, mutationIterator);

                Properties prop = new Properties();
                prop.setProperty(HOST_ID_PROPERTY, hostId.toString());
                prop.setProperty(DESCRIPTOR_TIMESTAMP_PROPERTY, Long.toString(descriptorTimestamp));
                prop.setProperty(CFID_PROPERTY, Schema.instance.getTableMetadata(KEYSPACE, TABLE).id.toString());
                prop.setProperty(CELLS_PROPERTY, Integer.toString(cells.get()));
                prop.setProperty(HASH_PROPERTY, Integer.toString(hash.get()));
                prop.store(new FileOutputStream(new File(dataDir, PROPERTIES_FILE).toJavaIOFile()),
                           "Hints, version " + FBUtilities.getReleaseVersionString());

                System.out.println("Done");
            }
            catch (Throwable t)
            {
                throw new RuntimeException(t);
            }
        }

        private void makeHintFile(File dir,
                                  HintsDescriptor descriptor,
                                  Iterator<Mutation> mutationIterator)
        {
            ByteBuffer buffer = ByteBuffer.allocateDirect(256 * 1024);

            try (HintsWriter writer = HintsWriter.create(dir, descriptor))
            {
                try (HintsWriter.Session session = writer.newSession(buffer))
                {
                    while (session.position() < maxLength && mutationIterator.hasNext())
                    {
                        Hint hint = Hint.create(mutationIterator.next(), System.currentTimeMillis(), Integer.MAX_VALUE);
                        session.append(hint);
                    }

                    System.out.println("Generating finished");
                }
            }
            catch (IOException ex)
            {
                throw new FSWriteError(ex, descriptor.fileName());
            }
            finally
            {
                MemoryUtil.clean(buffer);
            }
        }

        private static int hash(int hash, ByteBuffer bytes)
        {
            int shift = 0;
            for (int i = 0; i < bytes.limit(); i++)
            {
                hash += (bytes.get(i) & 0xFF) << shift;
                shift = (shift + 8) & 0x1F;
            }
            return hash;
        }

        private void initialize() throws Exception
        {
            try (FileInputStream fis = new FileInputStream("CHANGES.txt");
                 FileChannel fileChannel = fis.getChannel())
            {
                dataSource = ByteBuffer.allocateDirect((int) fileChannel.size());
                while (dataSource.hasRemaining())
                {
                    fileChannel.read(dataSource);
                }
                dataSource.flip();
            }

            SchemaLoader.loadSchema();

            TableMetadata metadata = TableMetadata.builder(KEYSPACE, TABLE)
                                                  .addPartitionKeyColumn("key", AsciiType.instance)
                                                  .addClusteringColumn("col", AsciiType.instance)
                                                  .addRegularColumn("val", BytesType.instance)
                                                  .addRegularColumn("val0", BytesType.instance)
                                                  .compression(SchemaLoader.getCompressionParameters())
                                                  .build();

            SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1), metadata);
        }

        private static ByteBuffer randomBytes(int quantity, ThreadLocalRandom tlr)
        {
            ByteBuffer slice = ByteBuffer.allocate(quantity);
            ByteBuffer source = dataSource.duplicate();
            source.position(tlr.nextInt(source.capacity() - quantity));
            source.limit(source.position() + quantity);
            slice.put(source);
            slice.flip();
            return slice;
        }
    }
}
