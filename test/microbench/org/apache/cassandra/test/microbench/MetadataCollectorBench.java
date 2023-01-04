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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ClusteringBoundary;
import org.apache.cassandra.db.ClusteringPrefix.Kind;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.jctools.util.Pow2;
import org.openjdk.jmh.annotations.*;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 10, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(1)
@State(Scope.Benchmark)
public class MetadataCollectorBench
{
    @Param({ "10" })
    int clusteringKeyNum;

    @Param({ "10000" })
    int datasetSize;
    int datumIndex;
    Cell<?>[] cells;
    Clustering<?>[] clusterings;
    ClusteringBound<?>[] clusteringBounds;
    ClusteringBoundary<?>[] clusteringBoundaries;
    MetadataCollector collector;

    @Setup
    public void setup()
    {
        TableMetadata.Builder tableMetadataBuilder = TableMetadata.builder("k", "t")
                                                                  .addPartitionKeyColumn("pk", LongType.instance)
                                                                  .addRegularColumn("rc", LongType.instance);
        for (int i = 0; i < clusteringKeyNum; i++)
            tableMetadataBuilder.addClusteringColumn("ck" + i, LongType.instance);
        TableMetadata tableMetadata = tableMetadataBuilder.build();
        collector = new MetadataCollector(tableMetadata.comparator);

        ColumnMetadata columnMetadata = tableMetadata.regularColumns().iterator().next();
        ThreadLocalRandom current = ThreadLocalRandom.current();
        datasetSize = Pow2.roundToPowerOfTwo(datasetSize);
        cells = new Cell[datasetSize];
        for (int i = 0; i < datasetSize; i++)
        {
            cells[i] = new BufferCell(columnMetadata, current.nextLong(0, Long.MAX_VALUE), current.nextInt(1, Integer.MAX_VALUE), Cell.NO_DELETION_TIME, null, null);
        }
        clusterings = new Clustering[datasetSize];
        clusteringBounds = new ClusteringBound[datasetSize];
        clusteringBoundaries = new ClusteringBoundary[datasetSize];
        ByteBuffer[] cks = new ByteBuffer[clusteringKeyNum];
        Kind[] clusteringBoundKinds = new Kind[]{ Kind.INCL_START_BOUND, Kind.INCL_END_BOUND, Kind.EXCL_START_BOUND, Kind.EXCL_END_BOUND };
        Kind[] clusteringBoundaryKinds = new Kind[]{ Kind.INCL_END_EXCL_START_BOUNDARY, Kind.EXCL_END_INCL_START_BOUNDARY };
        for (int i = 0; i < datasetSize; i++)
        {
            for (int j = 0; j < clusteringKeyNum; j++)
                cks[j] = LongType.instance.decompose(current.nextLong());
            clusterings[i] = Clustering.make(Arrays.copyOf(cks, cks.length));
            clusteringBounds[i] = ClusteringBound.create(clusteringBoundKinds[i % clusteringBoundKinds.length], clusterings[i]);
            clusteringBoundaries[i] = ClusteringBoundary.create(clusteringBoundaryKinds[i % clusteringBoundaryKinds.length], clusterings[i]);
        }

        System.gc();
        // shuffle array contents to ensure a more 'natural' layout
        for (int i = 0; i < datasetSize; i++)
        {
            int to = current.nextInt(0, datasetSize);
            Cell<?> temp = cells[i];
            cells[i] = cells[to];
            cells[to] = temp;
        }

        for (int i = 0; i < datasetSize; i++)
        {
            int to = current.nextInt(0, datasetSize);
            Clustering<?> temp = clusterings[i];
            clusterings[i] = clusterings[to];
            clusterings[to] = temp;
        }
    }

    @Benchmark
    public void updateCell()
    {
        collector.update(nextCell());
    }

    @Benchmark
    public void updateClustering()
    {
        collector.updateClusteringValues(nextClustering());
    }

    @Benchmark
    public void updateClusteringBound()
    {
        collector.updateClusteringValuesByBoundOrBoundary(nextClusteringBound());
    }

    @Benchmark
    public void updateClusteringBoundary()
    {
        collector.updateClusteringValuesByBoundOrBoundary(nextClusteringBoundary());
    }

    public Cell<?> nextCell()
    {
        return cells[datumIndex++ & (cells.length - 1)];
    }

    public Clustering<?> nextClustering()
    {
        return clusterings[datumIndex++ & (clusterings.length - 1)];
    }

    public ClusteringBound<?> nextClusteringBound()
    {
        return clusteringBounds[datumIndex++ & (clusteringBounds.length - 1)];
    }

    public ClusteringBoundary<?> nextClusteringBoundary()
    {
        return clusteringBoundaries[datumIndex++ & (clusteringBoundaries.length - 1)];
    }
}