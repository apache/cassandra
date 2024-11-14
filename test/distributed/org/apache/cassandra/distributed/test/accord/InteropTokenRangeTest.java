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

package org.apache.cassandra.distributed.test.accord;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;
import javax.annotation.Nullable;

import com.google.common.collect.Sets;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.service.consensus.TransactionalMode;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.dht.Murmur3Partitioner.LongToken.keyForToken;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.QUORUM;

@RunWith(Parameterized.class)
public class InteropTokenRangeTest extends TestBaseImpl
{
    @Nullable
    private final TransactionalMode mode;

    public InteropTokenRangeTest(@Nullable TransactionalMode mode)
    {
        this.mode = mode;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        List<Object[]> tests = new ArrayList<>(TransactionalMode.values().length + 1);
        tests.add(new Object[] {null});
        for (TransactionalMode mode : TransactionalMode.values())
            tests.add(new Object[] {mode});
        return tests;
    }

    @Test
    public void test() throws IOException
    {
        try (Cluster cluster = Cluster.build(1)
                                      .start())
        {
            init(cluster);
            String createTable = withKeyspace("CREATE TABLE %s.tbl (pk blob primary key)");
            if (mode != null)
                createTable += " WITH " + mode.asCqlParam();
            cluster.schemaChange(createTable);

            ICoordinator node = cluster.coordinator(1);
            NavigableSet<Long> tokens = tokens();
            for (long token : tokens)
                node.executeWithResult(withKeyspace("INSERT INTO %s.tbl (pk) VALUES (?)"), QUORUM, keyForToken(token));

            for (long token : tokens)
            {
                ByteBuffer pk = keyForToken(token);
                for (TokenOperator op : TokenOperator.values())
                {
                    Assertions.assertThat(tokens(node.executeWithResult(withKeyspace("SELECT * FROM %s.tbl WHERE " + op.condition), QUORUM, pk)))
                              .describedAs("Token %d with operator %s", token, op.condition)
                              .isEqualTo(op.expected(token, tokens));
                }

                for (TokenOperator lt : Arrays.asList(TokenOperator.lt, TokenOperator.lte))
                {
                    for (TokenOperator gt : Arrays.asList(TokenOperator.gt, TokenOperator.gte))
                    {
                        Assertions.assertThat(tokens(node.executeWithResult(withKeyspace("SELECT * FROM %s.tbl WHERE " + lt.condition + " AND " + gt.condition), QUORUM, pk, pk)))
                                  .describedAs("Token %d with operators %s / %s", token, lt.condition, gt.condition)
                                  .isEqualTo(Sets.intersection(lt.expected(token, tokens), gt.expected(token, tokens)));
                    }
                }

                Assertions.assertThat(tokens(node.executeWithResult(withKeyspace("SELECT * FROM %s.tbl WHERE token(pk) BETWEEN token(?) AND token(?)"), QUORUM, pk, pk)))
                          .describedAs("Token %d with operator token(pk) BETWEEN token(?) AND token(?)", token)
                          .isEqualTo(TokenOperator.eq.expected(token, tokens));
            }
        }
    }

    private static NavigableSet<Long> tokens(SimpleQueryResult result)
    {
        NavigableSet<Long> set = new TreeSet<>();
        while (result.hasNext())
            set.add(Murmur3Partitioner.instance.getToken(result.next().get("pk")).token);
        return set;
    }

    private enum TokenOperator
    {
        eq("token(pk) = token(?)") {
            @Override
            public NavigableSet<Long> expected(long token, NavigableSet<Long> tokens)
            {
                if (tokens.contains(token))
                    return new TreeSet<>(Collections.singleton(token));
                return Collections.emptyNavigableSet();
            }
        },
        lt("token(pk) < token(?)")
        {
            @Override
            public NavigableSet<Long> expected(long token, NavigableSet<Long> tokens)
            {
                return tokens.headSet(token, false);
            }
        },
        lte("token(pk) <= token(?)")
        {
            @Override
            public NavigableSet<Long> expected(long token, NavigableSet<Long> tokens)
            {
                return tokens.headSet(token, true);
            }
        },
        gt("token(pk) > token(?)")
        {
            @Override
            public NavigableSet<Long> expected(long token, NavigableSet<Long> tokens)
            {
                return tokens.tailSet(token, false);
            }
        },
        gte("token(pk) >= token(?)")
        {
            @Override
            public NavigableSet<Long> expected(long token, NavigableSet<Long> tokens)
            {
                return tokens.tailSet(token, true);
            }
        };
        ;

        private final String condition;

        TokenOperator(String s)
        {
            this.condition = s;
        }

        public abstract NavigableSet<Long> expected(long token, NavigableSet<Long> tokens);
    }

    private NavigableSet<Long> tokens()
    {
        NavigableSet<Long> set = new TreeSet<>();
//        set.add(Long.MIN_VALUE); // Murmur3Partitioner.LongToken.keyForToken does not support MIN_VALUE, but doesn't reject it... it actually produces MAX_VALUE...
        set.add(Long.MIN_VALUE + 1);
        set.add(0L);
        set.add(Long.MAX_VALUE);
        return set;
    }
}
