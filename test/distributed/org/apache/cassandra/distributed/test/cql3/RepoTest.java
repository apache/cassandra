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

package org.apache.cassandra.distributed.test.cql3;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.regex.Pattern;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.SimpleStatement;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.test.JavaDriverUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.assertj.core.api.Assertions;

/**
 * This test exists to help isolate issues with {@link SingleNodeTableWalkTest} and related classes.
 */
@Ignore
public class RepoTest extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(RepoTest.class);

    private static final Splitter CQL_SPLITTER = Splitter.on(';').trimResults().omitEmptyStrings();
    private static final Splitter NEW_LINE_SPLITTER = Splitter.on('\n').trimResults().omitEmptyStrings();
    private static final Splitter WHITESPACE_SPLITTER = Splitter.on(' ').trimResults().omitEmptyStrings();
    private static final String COMMENT_SEPERATOR = "--";
    private static final Pattern HISTORY_PREFIX = Pattern.compile("^([0-9]+):(.*)");

    @Test
    public void test() throws IOException
    {
        boolean multinode = true;
        ConsistencyLevel selectCL = multinode ? ConsistencyLevel.ALL : ConsistencyLevel.LOCAL_QUORUM;
        ConsistencyLevel mutationCL = multinode ? ConsistencyLevel.NODE_LOCAL : ConsistencyLevel.LOCAL_QUORUM;

        File schemaFile = new File("/tmp/repotest-schema.cql");
        File nonSelectHistory = new File("/tmp/repotest-non-select.history");
        String ks = "ks1";

        try (Cluster cluster = Cluster.build(multinode ? 3 : 1)
                                      .withConfig(c -> c.with(Feature.values())
                                                        .set("incremental_backups", false)
                                                        .set("range_request_timeout", "180s")
                                                        .set("read_request_timeout", "180s")
                                                        .set("write_request_timeout", "180s")
                                                        .set("native_transport_timeout", "180s")
                                                        .set("slow_query_log_timeout", "180s")
                                      ).start();
             var driver = JavaDriverUtils.create(cluster);
             var session = driver.connect())
        {
            // we don't allow setting null in yaml... but these configs support null!
            cluster.forEach(i ->  i.runOnInstance(() -> {
                // When values are large SAI will drop them... soooo... disable that... this test does not care about perf but correctness
                DatabaseDescriptor.getRawConfig().sai_frozen_term_size_warn_threshold = null;
                DatabaseDescriptor.getRawConfig().sai_frozen_term_size_fail_threshold = null;
            }));
            Map<Integer, Host> nodeToHost = new HashMap<>();
            for (Host host : driver.getMetadata().getAllHosts())
            {
                byte[] address = host.getBroadcastSocketAddress().getAddress().getAddress();
                Preconditions.checkState(address.length == 4);
                int node = address[3];
                var previous = nodeToHost.put(node, host);
                if (previous != null)
                    throw new IllegalStateException("This test only works with addresses shifted by nodeId and doesn't support port changes");
            }

            for (String cql : CQL_SPLITTER.split(Files.readString(schemaFile.toPath(), StandardCharsets.UTF_8)))
            {
                if (cql.startsWith("CREATE KEYSPACE"))
                {
                    if (multinode)
                    {
                        if (cql.contains("'replication_factor': 1"))
                            cql = cql.replace("'replication_factor': 1", "'replication_factor': 3");
                    }
                    else
                    {
                        if (cql.contains("'replication_factor': 3"))
                            cql = cql.replace("'replication_factor': 3", "'replication_factor': 1");
                    }
                }
                cluster.schemaChange(cql);
            }

            cluster.forEach(i -> i.nodetoolResult("disableautocompaction", ks, "tbl").asserts().success());

            BiConsumer<Integer, String> write = (nodeId, cql) -> {
                logger.info("Applying mutation {} against coordinator {}", cql, nodeId);
                var node = cluster.get(multinode ? nodeId : 1);
                if (mutationCL == ConsistencyLevel.NODE_LOCAL)
                {
                    node.executeInternal(cql);
                }
                else
                {
                    node.coordinator().execute(cql, mutationCL);
                }
            };

            for (String line : NEW_LINE_SPLITTER.split(Files.readString(nonSelectHistory.toPath(), StandardCharsets.UTF_8)))
            {
                if (line.startsWith("History:")) continue;
                var matcher = HISTORY_PREFIX.matcher(line);
                if (matcher.matches())
                {
                    line = matcher.group(2).trim();
                }
                if (line.startsWith("nodetool"))
                {
                    // nodetool flush ks1 tbl
                    // this runs on every node
                    var split = WHITESPACE_SPLITTER.splitToList(line);
                    var args = split.subList(1, split.size()).toArray(String[]::new);
                    cluster.forEach(i -> i.nodetoolResult(args).asserts().success());
                }
                else
                {
                    // its CQL
                    String cql;
                    int node;
                    int idx = line.lastIndexOf(COMMENT_SEPERATOR);
                    if (idx < 0)
                        throw new AssertionError("Unable to parse CQL line: " + line);
                    try
                    {
                        cql = line.substring(0, idx).trim();
                        node = Integer.parseInt(line.substring(idx + 2).trim().replace("on node", ""));
                    }
                    catch (Exception e)
                    {
                        throw new AssertionError("Unable to parse CQL line: " + line);
                    }
                    write.accept(node, cql);
                }
            }

            // 38: SELECT * FROM ks1.tbl WHERE s0 = 'c4Nw\u0002}' AND ck0 = '14:01:27.502156662' ALLOW FILTERING -- s0 ascii (indexed with SAI), ck0 time (indexed with SAI), on node3, fetch size 2147483647
            int selectNode = 3;
            String select = "SELECT * FROM ks1.tbl WHERE s0 = 'c4Nw\\u0002}' AND ck0 = '14:01:27.502156662'";

            SimpleStatement stmt = new SimpleStatement(select);
            stmt.setConsistencyLevel(JavaDriverUtils.toDriverCL(selectCL));
            stmt.setHost(nodeToHost.get(selectNode));
            var result = StatefulASTBase.BaseState.getRowsAsByteBuffer(session.execute(stmt));

            Assertions.assertThat(result).hasDimensions(1, 5);

//            // 396: SELECT * FROM ks1.tbl WHERE pk0 = '0.q\u0017wJ1' AND ck1 > 00000000-0000-1c00-b700-000000000000 -- ck1 timeuuid (reversed), indexed with SAI, on node1, fetch size 2147483647
//            SimpleStatement stmt = new SimpleStatement("SELECT * FROM ks1.tbl WHERE pk0 = '0.q\\u0017wJ1' AND ck1 > 00000000-0000-1c00-b700-000000000000");
//            stmt.setConsistencyLevel(JavaDriverUtils.toDriverCL(selectCL));
//            stmt.setHost(nodeToHost.get(1));
//            var result = StatefulASTBase.BaseState.getRowsAsByteBuffer(session.execute(stmt));
//            Assertions.assertThat(result).hasDimensions(1, 5);


//            // SELECT * FROM ks1.tbl WHERE ck1 <= -1.1792862E33 -- ck1 float, indexed with SAI, on node3, fetch size 5000
//            AbstractType<?> ck1Type = FloatType.instance;
//            ByteBuffer ck1UpperLimit = ck1Type.asCQL3Type().fromCQLLiteral("-1.1792862E33");
//            String cql = "SELECT ck1 FROM ks1.tbl WHERE ck1 <= -1.1792862E33";
//            SimpleStatement stmt = new SimpleStatement(cql);
//            stmt.setConsistencyLevel(JavaDriverUtils.toDriverCL(selectCL));
//            stmt.setFetchSize(5000);
//            var result = StatefulASTBase.BaseState.getRowsAsByteBuffer(session.execute(stmt));
//            for (var row : result)
//            {
//                ByteBuffer ck1 = row[0];
//                int rc = ck1Type.compare(ck1, ck1UpperLimit);
//                if (rc > 0)
//                    throw new AssertionError("Unexpected ck1 value: " + ck1Type.asCQL3Type().toCQLLiteral(ck1));
//            }
//
//            // test error
//            /*
//Caused by: java.lang.AssertionError: Unexpected rows found:
//pk0              | pk1         | ck0                                  | ck1           | s0             | v0                                   | v1    | v2         | v3     | v4
//0x3db7b69ecdd6a6 | -1459423004 | 00000000-0000-4200-b600-000000000000 | 2.3472485E-29 | -2.59839216E17 | 00000000-0000-4f00-a500-000000000000 | true  | 1355516395 | 0x5d   | '33.54.24.0'
//0x3db7b69ecdd6a6 | -1459423004 | 00000000-0000-4a00-8400-000000000000 | -4.3491496E7  | -2.59839216E17 | null                                 | true  | null       | 0x9c50 | '230.216.82.197'
//0x3db7b69ecdd6a6 | -1459423004 | 00000000-0000-4800-b200-000000000000 | 0.02575966    | -2.59839216E17 | 00000000-0000-4700-ac00-000000000000 | false | 1758201503 | 0xdf   | '40a5:cfbf:23ef:c168:de52:4ac4:c56d:6795'
//0x3db7b69ecdd6a6 | -1459423004 | 00000000-0000-4f00-a100-000000000000 | -2.691734E-5  | -2.59839216E17 | 00000000-0000-4a00-b700-000000000000 | null  | null       | null   | '168.70.3.14'
//
//Expected:
//pk0              | pk1         | ck0                                  | ck1           | s0             | v0                                   | v1    | v2   | v3     | v4
//0x3db7b69ecdd6a6 | -1459423004 | 00000000-0000-4600-9f00-000000000000 | -3.854987E34  | -2.59839216E17 | null                                 | null  | null | 0x003c | '126.229.32.3'
//0x3027b5aa00a577 | -652702739  | 00000000-0000-4b00-8400-000000000000 | -1.1792862E33 | -2.7431481E-8  | 00000000-0000-4a00-9b00-000000000000 | false | null | null   | null
//0x3027b5aa00a577 | -652702739  | 00000000-0000-4c00-9a00-000000000000 | -7.706793E37  | -2.7431481E-8  | null                                 | false | null | null   | null
//             */
////            Assertions.assertThat(result).hasDimensions(3, 10);
        }
    }
}
