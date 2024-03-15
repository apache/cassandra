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

package org.apache.cassandra.index.sai.cql;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;
import javax.annotation.Nullable;

import accord.utils.Gen;
import accord.utils.Property;
import org.agrona.collections.IntArrayList;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.sai.SAITester;
import org.assertj.core.api.Assertions;

import static accord.utils.Property.qt;

public abstract class AbstractSimpleEqTestBase extends SAITester
{
    static
    {
        // The info table gets updated and force-flushed every time we truncate, so disable that and avoid the overhead:
        CassandraRelevantProperties.UNSAFE_SYSTEM.setBoolean(true);

        // Ignore SAI timeouts as this is just validating the read/write logic of the index:
        CassandraRelevantProperties.SAI_TEST_DISABLE_TIMEOUT.setBoolean(true);
    }

    protected void test(AbstractType<?> type, @Nullable Long seed, int examples, Gen<Gen<ByteBuffer>> distribution)
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, value " + type.asCQL3Type() + ')');
        disableCompaction(KEYSPACE);
        createIndex(String.format(CREATE_INDEX_TEMPLATE, "value"));

        Property.ForBuilder builder = qt().withExamples(examples);
        if (seed != null)
            builder = builder.withSeed(seed);

        builder.check(rs -> {
            execute("TRUNCATE %s");

            Gen<ByteBuffer> support = distribution.next(rs);
            Map<ByteBuffer, IntArrayList> termIndex = new TreeMap<>();

            for (int i = 0; i < 1000; i++)
            {
                ByteBuffer term = support.next(rs);
                execute("INSERT INTO %s (pk, value) VALUES (?, ?)", i, term);
                termIndex.computeIfAbsent(term, ignore -> new IntArrayList()).addInt(i);
            }
            
            flush();

            for (var e : termIndex.entrySet())
            {
                ByteBuffer term = e.getKey();
                IntArrayList expected = e.getValue();
                UntypedResultSet result = execute("SELECT pk, value FROM %s WHERE value=?", term);
                IntArrayList actual = new IntArrayList(expected.size(), -1);
                for (var row : result)
                {
                    ByteBuffer readValue = row.getBytes("value");
                    Assertions.assertThat(readValue).describedAs("%s != %s", type.compose(term), type.compose(readValue)).isEqualTo(term);
                    actual.add(row.getInt("pk"));
                }
                expected.sort(Comparator.naturalOrder());
                actual.sort(Comparator.naturalOrder());
                Assertions.assertThat(actual).describedAs("Unexpected partitions for term %s", type.compose(term)).isEqualTo(expected);
            }
        });
    }
}