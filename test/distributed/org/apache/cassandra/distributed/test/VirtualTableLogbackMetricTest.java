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

package org.apache.cassandra.distributed.test;

import com.google.common.collect.Sets;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.schema.SchemaConstants;

import static java.lang.String.format;
import static org.apache.cassandra.config.CassandraRelevantProperties.LOGBACK_CONFIGURATION_FILE;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ONE;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class VirtualTableLogbackMetricTest extends AbstractVirtualLogsTableTest
{
    @Override
    public String getTableName()
    {
        return format("%s.%s", SchemaConstants.VIRTUAL_METRICS, "logback_metrics_group");
    }

    @Test
    public void testVTableOutputWhenDisable() throws Throwable
    {
        // logback-dtest.xml does not configure LogbackMetrics; reuse it to avoid creating another file.
        try (WithProperties properties = new WithProperties().set(LOGBACK_CONFIGURATION_FILE, "test/conf/logback-dtest.xml");
             Cluster cluster = Cluster.build(1)
                                      .withConfig(c -> c.with(Feature.values()))
                                      .start();
        )
        {
            SimpleQueryResult simpleQueryResult = cluster.coordinator(1).executeWithResult(query("select * from %s"), ONE);
            Object[][] rows = simpleQueryResult.toObjectArrays();
            assertEquals(0, rows.length);
        }
    }

    @Test
    public void testVTableOutputWhenEnable() throws Throwable
    {
        try (WithProperties properties = new WithProperties().set(LOGBACK_CONFIGURATION_FILE, "test/conf/logback-dtest_with_logback_metric_appender.xml");
             Cluster cluster = Cluster.build(1)
                                      .withConfig(c -> c.with(Feature.values()))
                                      .start();
        )
        {
            SimpleQueryResult simpleQueryResult = cluster.coordinator(1).executeWithResult(query("select * from %s"), ONE);
            Object[][] rows = simpleQueryResult.toObjectArrays();
            assertEquals(6, rows.length);

            simpleQueryResult.forEachRemaining(row -> {
                String name = row.getString("name");
                String scope = row.getString("scope");
                String type = row.getString("type");
                String value = row.getString("value");

                assertTrue(name.startsWith("org.apache.cassandra.metrics.LogbackMetrics."));
                String suffix = name.substring("org.apache.cassandra.metrics.LogbackMetrics.".length());
                assertTrue(Sets.newHashSet("all", "debug", "error", "info", "trace", "warn").contains(suffix));
                assertEquals("undefined", scope);
                assertEquals("meter", type);
                assertThat(Integer.parseInt(value)).isGreaterThanOrEqualTo(0);
            });
        }
    }
}
