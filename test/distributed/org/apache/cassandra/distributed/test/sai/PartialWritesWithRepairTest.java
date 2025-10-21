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

package org.apache.cassandra.distributed.test.sai;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.shared.ClusterUtils.Range;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class PartialWritesWithRepairTest extends TestBaseImpl
{
    @Test
    public void test() throws IOException
    {
        try (Cluster cluster = Cluster.build(2)
                .withConfig(c -> c.with(Feature.values()))
                .start())
        {
            init(cluster);
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk vector<bigint, 2>, ck int, s1 int static, v1 int, v2 int, PRIMARY KEY (pk, ck))"));
            cluster.schemaChange(withKeyspace("CREATE INDEX ON %s.tbl(s1) USING 'sai'"));
            cluster.schemaChange(withKeyspace("CREATE INDEX ON %s.tbl(v1) USING 'sai'"));
            cluster.schemaChange(withKeyspace("CREATE INDEX ON %s.tbl(v2) USING 'sai'"));
            IInvokableInstance node1 = cluster.get(1);
            IInvokableInstance node2 = cluster.get(2);
            // see org.apache.cassandra.service.StorageService.repair
            List<Range> partialRanges = ClusterUtils.getPrimaryRanges(node1, KEYSPACE);
            var completeRanges = completeRanges(partialRanges);

            // write to each column for the complete set
            // avoid writing to one of the columns for the partial set
            for (var range : completeRanges)
            {
                ByteBuffer pk = key(range);
                node2.executeInternal(withKeyspace("INSERT INTO %s.tbl(pk, ck, s1, v1, v2) VALUES (?, ?, ?, ?, ?)"), pk, 0, 0, 0, 0);
                node2.executeInternal(withKeyspace("INSERT INTO %s.tbl(pk, ck, s1, v1, v2) VALUES (?, ?, ?, ?, ?)"), pk, 1, 0, 1, 1);
            }
            for (var range : partialRanges)
            {
                ByteBuffer pk = key(range);
                node2.executeInternal(withKeyspace("INSERT INTO %s.tbl(pk, ck, v1) VALUES (?, ?, ?)"), pk, 0, 0);
                node2.executeInternal(withKeyspace("INSERT INTO %s.tbl(pk, ck, v1) VALUES (?, ?, ?)"), pk, 1, 1);
            }

            node1.nodetoolResult("repair", KEYSPACE, "-pr").asserts().success();
        }
    }

    private static ByteBuffer key(Range range)
    {
        return Murmur3Partitioner.LongToken.keyForToken(range.right());
    }

    private static List<Range> completeRanges(List<Range> ranges)
    {
        ranges.sort(Comparator.comparingLong(Range::left));
        List<Range> list = new ArrayList<>();
        Range previous = ranges.get(0);
        if (previous.left() != Long.MIN_VALUE)
            list.add(new Range(Long.MIN_VALUE, ranges.get(0).left()));
        for (int i = 1; i < ranges.size(); i++)
        {
            Range next = ranges.get(i);
            if (!previous.right.equals(next.left))
                list.add(new Range(previous.right, next.left));
            previous = next;
        }
        return list;
    }
}
