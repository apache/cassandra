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

package org.apache.cassandra.tools;

import accord.local.RedundantBefore;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import accord.utils.Invariants;
import org.apache.cassandra.config.AccordSpec;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.journal.Descriptor;
import org.apache.cassandra.journal.DumpUtil;
import org.apache.cassandra.journal.StaticSegment;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.service.accord.AccordJournal;
import org.apache.cassandra.service.accord.AccordKeyspace;
import org.apache.cassandra.service.accord.JournalKey;
import org.apache.cassandra.service.accord.serializers.Version;
import org.apache.cassandra.tcm.ClusterMetadataService;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import static accord.api.Journal.Load.ALL;
import static com.google.common.base.Throwables.getStackTraceAsString;
import static org.apache.cassandra.config.DatabaseDescriptor.setAccordJournalDirectory;

/**
 * Standalone Accord Journal Util. Useful for inspecting journals on remote nodes in cases of failures
 * As a convenience, you can build the dtest-jar and scp it to the server, and run the tool:

  java --add-exports java.base/jdk.internal.misc=ALL-UNNAMED \
       --add-exports java.base/jdk.internal.ref=ALL-UNNAMED \
       --add-exports java.base/sun.nio.ch=ALL-UNNAMED \
       --add-exports java.management.rmi/com.sun.jmx.remote.internal.rmi=ALL-UNNAMED \
       --add-exports java.rmi/sun.rmi.registry=ALL-UNNAMED \
       --add-exports java.rmi/sun.rmi.server=ALL-UNNAMED \
       --add-exports java.rmi/sun.rmi.transport.tcp=ALL-UNNAMED \
       --add-exports java.sql/java.sql=ALL-UNNAMED \
     \
       --add-opens java.base/java.lang.module=ALL-UNNAMED \
       --add-opens java.base/java.net=ALL-UNNAMED \
       --add-opens java.base/jdk.internal.loader=ALL-UNNAMED \
       --add-opens java.base/jdk.internal.ref=ALL-UNNAMED \
       --add-opens java.base/jdk.internal.reflect=ALL-UNNAMED \
       --add-opens java.base/jdk.internal.math=ALL-UNNAMED \
       --add-opens java.base/jdk.internal.module=ALL-UNNAMED \
       --add-opens java.base/jdk.internal.util.jar=ALL-UNNAMED \
       --add-opens jdk.management/com.sun.management.internal=ALL-UNNAMED \
       --add-opens jdk.management/com.sun.beans.introspect=ALL-UNNAMED \
       --add-opens jdk.management.jfr/jdk.management.jfr=ALL-UNNAMED \
       --add-opens java.desktop/com.sun.beans.introspect=ALL-UNNAMED \
       -cp ./dtest-5.1.jar \
       org.apache.cassandra.tools.StandaloneJournalUtil \
       dump_journal --construct --skip-errors --sstables /tmp/journals/journal-c3412dbac2923051b7914f7b0b79a166/ --journal-segments /tmp/journals/accord_journal/

 *
**/
@Command(name = "journal_util",
         mixinStandardHelpOptions = true,
         description = "Standalone Journal Util",
         subcommands = { StandaloneJournalUtil.DumpSegments.class, StandaloneJournalUtil.DumpJournal.class })
public class StandaloneJournalUtil implements Runnable
{
    private static final Output output = Output.CONSOLE;

    public static void main(String... args)
    {
        Util.initDatabaseDescriptor();
        DatabaseDescriptor.setPartitioner("org.apache.cassandra.dht.Murmur3Partitioner");
        AccordKeyspace.TABLES = Tables.of(AccordKeyspace.journalMetadata("journal", false));
        ClusterMetadataService.empty(Keyspaces.of(AccordKeyspace.metadata()));

        CommandLine cli = new CommandLine(StandaloneJournalUtil.class)
                .setExecutionExceptionHandler((ex, cmd, parseResult) -> {
                    err(ex);
                    return 2;
                });
        int status = cli.execute(args);
        System.exit(status);
    }

    @Override
    public void run()
    {
        CommandLine.usage(this, output.out);
    }

    protected static void err(Throwable e)
    {
        output.err.println("error: " + e.getMessage());
        output.err.println("-- StackTrace --");
        output.err.println(getStackTraceAsString(e));
    }

    @Command(name = "dump_segments", description = "Dump journal segments")
    public static class DumpSegments implements Runnable
    {
        @Option(names = {"-d", "--dir"}, description = "Directory to find journal segments")
        public String dir;

        @Option(names = {"-p", "--pattern"}, description = "Kind to filter by")
        public String pattern;

        @Option(names = {"-k", "--kind"}, description = "Kind to filter by")
        public String kind;

        @Option(names = {"-t", "--txnid"}, description = "Transaction id to filter by")
        public String txnId;

        @Option(names = {"-m", "--metadata-only"}, description = "Only dump metadata file contents")
        public boolean metadataOnly;

        public void run()
        {
            PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + pattern);
            try
            {
                Files.list(Path.of(dir)).filter(Files::isRegularFile).filter(p -> matcher.matches(p.getFileName())).sorted().forEach(path -> {
                    Descriptor descriptor = Descriptor.fromFile(new File(path));
                    DumpUtil.dumpMetadata(descriptor, output.out::println);
                    if (metadataOnly)
                        return;

                    StaticSegment<JournalKey, Object> segment = DumpUtil.open(descriptor, JournalKey.SUPPORT);
                    segment.forEachRecord((segment1, position, key, buffer, userVersion) -> {
                        if (kind != null && key.type != JournalKey.Type.valueOf(kind))
                            return;

                        if (txnId != null && !TxnId.fromString(txnId).equals(key.id))
                            return;

                        try (DataInputBuffer in = new DataInputBuffer(buffer, false))
                        {
                            Object v = key.type.serializer.deserialize(key, in, Version.V1);
                            output.out.printf("%s: %s%n", key, v);
                        }
                        catch (Throwable t)
                        {
                            throw new RuntimeException(String.format("Error reading key %s in segment %s at position %d: %s",
                                                                key, segment1, position, t.getMessage()), t);
                        }
                    });
                });
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }

        }
    }

    @Command(name = "dump_journal", description = "Dump journal")
    public static class DumpJournal implements Runnable
    {
        @Option(names = {"-s", "--sstables"}, description = "Path to sstables")
        public String sstables;

        @Option(names = {"-j", "--journal-segments"}, description = "Path to journal segments")
        public String journalSegments;

        @Option(names = {"-k", "--kind"}, description = "Kind to filter by")
        public String kind;

        @Option(names = {"-t", "--txnid"}, description = "Transaction id to filter by")
        public String txnId;

        @Option(names = {"--since"}, description = "Filter transactions since this timestamp (inclusive)")
        public String since;

        @Option(names = {"--until"}, description = "Filter transactions until this timestamp (inclusive)")
        public String until;

        @Option(names = {"-e", "--skip-errors"}, description = "Skip errors: 'true' to skip all, or comma-separated exception class names to skip specific types")
        public String skipErrors;

        @Option(names = {"-c", "--construct"}, description = "Construct entry")
        public boolean construct;


        public void run()
        {
            if (sstables == null && journalSegments == null)
                throw new IllegalArgumentException("Either --sstables or --journal-segments must be provided");

            Timestamp txnId = this.txnId != null ? TxnId.fromString(this.txnId) : null;
            Timestamp sinceTimestamp = this.since != null ? TxnId.fromString(this.since) : null;
            Timestamp untilTimestamp = this.until != null ? TxnId.fromString(this.until) : null;

            boolean skipAllErrors;
            Set<String> skipExceptionTypes = new HashSet<>();

            if (skipErrors != null && !skipErrors.trim().isEmpty())
            {
                String trimmed = skipErrors.trim();
                if ("true".equalsIgnoreCase(trimmed))
                {
                    skipAllErrors = true;
                }
                else
                {
                    skipAllErrors = false;
                    String[] types = trimmed.split(",");
                    for (String type : types)
                    {
                        skipExceptionTypes.add(type.trim());
                    }
                }
            }
            else
            {
                skipAllErrors = false;
            }

            if (journalSegments == null)
            {
                try
                {
                    journalSegments = Files.createTempDirectory("dump_journal").getFileName().toString();
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
            }

            setAccordJournalDirectory(journalSegments);
            Keyspace.setInitialized();
            AccordJournal journal = new AccordJournal(new AccordSpec.JournalSpec().setFlushPeriod(new DurationSpec.IntMillisecondsBound("1500ms")), new File(journalSegments).parent(), Keyspace.open(SchemaConstants.ACCORD_KEYSPACE_NAME).getColumnFamilyStore(AccordKeyspace.JOURNAL));

            Keyspace ks = Schema.instance.getKeyspaceInstance("system_accord");
            ColumnFamilyStore cfs = ks.getColumnFamilyStore("journal");
            if (sstables != null)
                cfs.importNewSSTables(Collections.singleton(sstables), false, false, false, false, false, false, true);

            Map<Integer, RedundantBefore> cache = new HashMap<>();
            journal.start(null);
            journal.forEach(key -> processKey(cache, journal, key, txnId, sinceTimestamp, untilTimestamp, skipAllErrors, skipExceptionTypes));
        }

        private void processKey(Map<Integer, RedundantBefore> redundantBeforeCache, AccordJournal journal, JournalKey key, Timestamp txnId, Timestamp minTimestamp, Timestamp maxTimestamp, boolean skipAllErrors, Set<String> skipExceptionTypes)
        {
            if (kind != null && key.type != JournalKey.Type.valueOf(kind))
                return;

            if (txnId != null && !txnId.equals(key.id))
                return;

            // Apply since filtering (inclusive)
            if ((minTimestamp != null && key.id.compareTo(minTimestamp) < 0) ||
                (maxTimestamp != null && key.id.compareTo(maxTimestamp) > 0))
                return;

            try
            {
                switch (key.type)
                {
                    case COMMAND_DIFF:
                    {
                        AtomicInteger counter = new AtomicInteger();
                        output.out.println(key + " " + key.id.toStandardString());

                        output.out.println("Individual entries:");
                        journal.forEachEntry(key, (in, userVersion) -> {
                            AccordJournal.Builder builder = new AccordJournal.Builder(key.id, ALL);
                            builder.deserializeNext(in, userVersion);
                            output.out.println(String.format("\t%s", builder.toString("\n\t\t")));
                            counter.getAndIncrement();
                        });

                        if (construct)
                        {
                            AccordJournal.Builder builder = new AccordJournal.Builder(key.id, ALL);
                            journal.forEachEntry(key, builder::deserializeNext);
                            output.out.println("Reconstructed\n\t\t" + builder.construct(redundantBeforeCache.computeIfAbsent(key.commandStoreId, k -> journal.loadRedundantBefore(key.commandStoreId))));
                        }

                        break;
                    }
                    case REDUNDANT_BEFORE:
                    case DURABLE_BEFORE:
                    case SAFE_TO_READ:
                    case BOOTSTRAP_BEGAN_AT:
                    case RANGES_FOR_EPOCH:
                    case TOPOLOGY_UPDATE:
                    {
                        Object image = journal.readAll(key);
                        output.out.println(image);
                    }
                }
            }
            catch (Throwable t)
            {
                boolean shouldSkip = false;

                if (skipAllErrors)
                {
                    shouldSkip = true;
                }
                else if (!skipExceptionTypes.isEmpty())
                {
                    String exceptionClassName = t.getClass().getSimpleName();
                    String fullExceptionClassName = t.getClass().getName();

                    shouldSkip = skipExceptionTypes.contains(exceptionClassName) ||
                                 skipExceptionTypes.contains(fullExceptionClassName);
                }

                if (!shouldSkip)
                    throw t;

                throw new RuntimeException(String.format("Error reading key %s: %s", key, t.getMessage()), t);
            }
        }
    }

    @Command(name = "load", description = "Load item from journal")
    public static class Load implements Runnable
    {
        @Option(names = {"-s", "--sstables"}, description = "Path to sstables")
        public String sstables;

        @Option(names = {"-j", "--journal-segments"}, description = "Path to journal segments")
        public String journalSegments;

        @Option(names = {"-k", "--kind"}, description = "Kind to filter by")
        public String kind;

        @Option(names = {"-c", "--command-store-id"}, description = "Command Store id")
        public String commandStoreId;

        public void run()
        {
            if (sstables == null && journalSegments == null)
                throw new IllegalArgumentException("Either --sstables or --journal-segments must be provided");

            if (journalSegments == null)
            {
                try
                {
                    journalSegments = Files.createTempDirectory("dump_journal").getFileName().toString();
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
            }

            setAccordJournalDirectory(journalSegments);
            Keyspace.setInitialized();
            AccordJournal journal = new AccordJournal(new AccordSpec.JournalSpec().setFlushPeriod(new DurationSpec.IntMillisecondsBound("1500ms")), new File(journalSegments).parent(), Keyspace.open(SchemaConstants.ACCORD_KEYSPACE_NAME).getColumnFamilyStore(AccordKeyspace.JOURNAL));

            Keyspace ks = Schema.instance.getKeyspaceInstance("system_accord");
            ColumnFamilyStore cfs = ks.getColumnFamilyStore("journal");
            if (sstables != null)
                cfs.importNewSSTables(Collections.singleton(sstables), false, false, false, false, false, false, true);

            journal.start(null);
            if (kind.toString().equals(JournalKey.Type.REDUNDANT_BEFORE.toString()))
            {
                Invariants.require(commandStoreId != null);
                int commandStoreId = Integer.parseInt(this.commandStoreId);
                output.out.println(journal.loadRedundantBefore(commandStoreId));
            }
        }
    }
}
