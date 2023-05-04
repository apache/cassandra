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

package org.apache.cassandra.db.compaction;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.LongPredicate;

import com.google.common.base.Throwables;
import com.google.common.collect.Sets;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableRewriter;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.OutputHandler;
import org.hsqldb.Column;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;

public class Downgrader
{
    private final ColumnFamilyStore cfs;
    private final SSTableReader sstable;
    private final LifecycleTransaction transaction;
    private final File directory;

    private final CompactionController controller;
    private final CompactionStrategyManager strategyManager;
    private final long estimatedRows;

    private final SSTableFormat.Type inputType;
    private final SSTableFormat.Type outputType;
    private final Version outputVersion;

    private final OutputHandler outputHandler;

    private static  Map<String,Map<String,Function<ColumnMetadata,ColumnMetadata>>> mappers = new HashMap<>();

    static {
        register("system","local", Downgrader::localMapper );
    }

    private static void register(String keyspace, String name, Function<ColumnMetadata,ColumnMetadata> func) {

        Map<String,Function<ColumnMetadata,ColumnMetadata>> mapper = mappers.get(keyspace);
        if (mapper == null) {
            mapper = new HashMap<>();
            mappers.put(keyspace,mapper);
        }
        mapper.put( name", func );
    }

    public static SSTableFormat.Type forWriting(String version)
    {
        String[] v3 =
        { "ma" , "mb" , "mc" , "md" , "me" , "mf" , "mg" , "mh" , "mi" , "mj" , "mk" ,
          "ml" , "mm" , "mn" , "mo" , "mp" , "mq" , "mr" , "ms" , "mt" , "mu" , "mv" , "mw" , "mx" , "my" , "mz" ,
          "na" };

        String[] v4 =
        { "nb", "nc", "nd", "ne", "nf", "ng", "nh", "ni", "nj", "nk", "nl", "nn", "nn", "no", "np", "nq", "nr", "ns",
          "nt", "nu", "nv", "nw", "nx", "ny", "nz" };

        if (version.compareTo( v3[v3.length-1]) <= 0) {
            for( String s : v3)
            {
                if (s.equals(version))
                {
                    return SSTableFormat.Type.V3;
                }
            }
        }

        if (version.compareTo( v4[v4.length-1]) <= 0) {
            for( String s : v4)
            {
                if (s.equals(version))
                {
                    return SSTableFormat.Type.BIG;
                }
            }
        }
        throw new IllegalArgumentException("No version constant " + version);
    }

    static ColumnMetadata localMapper(ColumnMetadata orig)  {
        List<String> remove = Arrays.asList("broadcast_port", "listen_port", "rpc_port");
        if (remove.contains(orig.name.toString())) {
            return null;
        }
        return orig;
    }


    public Downgrader(String version, ColumnFamilyStore cfs, LifecycleTransaction txn, OutputHandler outputHandler, SSTableFormat.Type sourceFormat)
    {
        this.cfs = cfs;
        this.transaction = txn;
        this.sstable = txn.onlyOne();
        this.outputHandler = outputHandler;

        this.inputType = sourceFormat;
        this.outputType = forWriting(version);
        this.outputVersion = outputType.info.getVersion(version);

        this.directory = new File(sstable.getFilename()).parent();

        this.controller = new Downgrader.DowngradeController(cfs);

        this.strategyManager = cfs.getCompactionStrategyManager();
        long estimatedTotalKeys = Math.max(cfs.metadata().params.minIndexInterval, SSTableReader.getApproximateKeyCount(Arrays.asList(this.sstable)));
        long estimatedSSTables = Math.max(1, SSTableReader.getTotalBytes(Arrays.asList(this.sstable)) / strategyManager.getMaxSSTableBytes());
        this.estimatedRows = (long) Math.ceil((double) estimatedTotalKeys / estimatedSSTables);
    }



    private SSTableWriter createCompactionWriter(StatsMetadata metadata)
    {
        MetadataCollector sstableMetadataCollector = new MetadataCollector(cfs.getComparator());
        sstableMetadataCollector.sstableLevel(sstable.getSSTableLevel());
        Descriptor newDescriptor = new Descriptor(outputVersion,
                                                      directory,
                                                      cfs.keyspace.getName(),
                                                      cfs.name,
                                                      cfs.getNextsstableId(),
                                                      outputType);
            assert !newDescriptor.fileFor(Component.DATA).exists();


        return SSTableWriter.create(newDescriptor,
                                    estimatedRows,
                                    metadata.repairedAt,
                                    metadata.pendingRepair,
                                    metadata.isTransient,
                                    cfs.metadata,
                                    sstableMetadataCollector,
                                    SerializationHeader.make(cfs.metadata(), Sets.newHashSet(sstable)),
                                    cfs.indexManager.listIndexes(),
                                    transaction);
    }

    Function<ColumnMetadata,ColumnMetadata> getColumnMapper(String keyspace, String name) {
        Function<ColumnMetadata,ColumnMetadata> columnMapper = null;
        Map<String,Function<ColumnMetadata,ColumnMetadata>> mapper = mappers.get(keyspace);
        if (mapper != null) {
            columnMapper = mapper.get(name);
        }
        return columnMapper != null ? columnMapper : (f) -> f;
    }

    TableMetadataRef rewrite(TableMetadataRef original) {

        TableMetadata origMeta = original.get();
        TableMetadata.Builder builder = original.get().unbuild();
        Function<ColumnMetadata,ColumnMetadata> columnMapper = getColumnMapper(original.keyspace, original.name);
        for (ColumnMetadata colMeta  : builder.columns())
        {
            ColumnMetadata newColMeta = columnMapper.apply(colMeta);
            if (newColMeta == null) {
                builder.dropColumn(colMeta.name);
            } else {
                builder.replaceColumn( colMeta.name, newColMeta );
            }
        }
        return TableMetadataRef.forOfflineTools(builder.build());
    }

    public void downgrade(boolean keepOriginals)
    {
        outputHandler.output("Downgrading " + sstable);
        int nowInSec = FBUtilities.nowInSeconds();
        try (SSTableRewriter writer = SSTableRewriter.construct(cfs, transaction, keepOriginals, CompactionTask.getMaxDataAge(transaction.originals()));
             AbstractCompactionStrategy.ScannerList scanners = strategyManager.getScanners(transaction.originals());
             CompactionIterator iter = new CompactionIterator(transaction.opType(), scanners.scanners, controller, nowInSec, nextTimeUUID()))
        {
            writer.switchWriter(createCompactionWriter(sstable.getSSTableMetadata()));
            iter.setTargetDirectory(writer.currentWriter().getFilename());
            while (iter.hasNext())
                writer.append(iter.next());

            writer.finish();
            outputHandler.output("Downgrade of " + sstable + " complete.");
        }
        catch (Exception e)
        {
            Throwables.throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
        finally
        {
            controller.close();
        }
    }

    private static class DowngradeController extends CompactionController
    {
        public DowngradeController(ColumnFamilyStore cfs)
        {
            super(cfs, Integer.MAX_VALUE);
        }

        @Override
        public LongPredicate getPurgeEvaluator(DecoratedKey key)
        {
            return time -> false;
        }
    }

    private static class TableMapper {
        Function<ColumnMetadata,ColumnMetadata> columnMapper;
    }

}
