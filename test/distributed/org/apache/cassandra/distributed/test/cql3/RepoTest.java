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
import java.util.function.BiConsumer;
import java.util.regex.Pattern;

import com.google.common.base.Splitter;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final String COMMENT_SEPERATOR = "--";
    private static final Pattern HISTORY_PREFIX = Pattern.compile("^([0-9]+):(.*)");

    @Test
    public void test() throws IOException
    {
        boolean multinode = false;
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
                //TODO (now): nodetool support
                if (true)
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

            //  -- ck1 float, indexed with SAI, on node1, fetch size 10
            SimpleStatement stmt = new SimpleStatement("SELECT * FROM ks1.tbl WHERE pk0 = 00000000-0000-1a00-9a00-000000000000 AND pk1 = 0x43c4 AND ck1 = -4662398.5");
            stmt.setConsistencyLevel(JavaDriverUtils.toDriverCL(selectCL));
            stmt.setFetchSize(10);
            var result = StatefulASTBase.BaseState.getRowsAsByteBuffer(session.execute(stmt));

            // test error
//        Caused by: java.lang.AssertionError: Unexpected rows found:
//        pk0                                  | pk1    | ck0           | ck1           | s0                                   | v0                                        | v1                                       | v2                   | v3
//            00000000-0000-1a00-9a00-000000000000 | 0x43c4 | 4.429273E27   | 1.079069E-27  | 00000000-0000-1e00-a800-000000000000 | 'c94d:711c:637a:b57d:871:63ed:eccf:b326'  | 'BC4<=[A*;q'                             | null                 | 00000000-0000-4100-b200-000000000000
//            00000000-0000-1a00-9a00-000000000000 | 0x43c4 | -8.426509E26  | -7.034981E-18 | 00000000-0000-1e00-a800-000000000000 | '63b4:2e9b:8a42:52f6:6f96:ba1:a154:5f47'  | '|Vp'                                    | '13:50:58.890279934' | 00000000-0000-4200-a900-000000000000
//            00000000-0000-1a00-9a00-000000000000 | 0x43c4 | 1.4277632E-17 | 1.4851762E-14 | 00000000-0000-1e00-a800-000000000000 | '1f4a:9b86:63e6:4cb4:41ec:8a5a:35a5:c2d1' | null                                     | '19:02:44.792645046' | 00000000-0000-4c00-b900-000000000000
//            00000000-0000-1a00-9a00-000000000000 | 0x43c4 | 2.800979E-6   | -4.773874E-16 | 00000000-0000-1e00-a800-000000000000 | '404c:4877:5f2f:1862:b0f7:ef78:b77b:8ed0' | 'l|#8Q\u000C:5\u0011a_e\u0002_1S\u0005a' | '07:24:14.125849387' | 00000000-0000-4000-9e00-000000000000
//
//            Expected:
//            pk0                                  | pk1    | ck0            | ck1        | s0                                   | v0   | v1   | v2   | v3
//            00000000-0000-1a00-9a00-000000000000 | 0x43c4 | -1.9557544E-23 | -4662398.5 | 00000000-0000-1e00-a800-000000000000 | null | null | null | null
            Assertions.assertThat(result).hasDimensions(1, 9);
        }
    }
}
