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

package org.apache.cassandra.distributed.test.thresholds;

import java.io.IOException;
import java.util.List;

import org.junit.*;

import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.config.DatabaseDescriptor;

import static org.apache.cassandra.config.DataStorageSpec.DataStorageUnit.KIBIBYTES;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * ReadSize client warn/abort is coordinator only, so the fact ClientMetrics is coordinator only does not
 * impact the user experience
 */
public class CoordinatorReadSizeWarningTest extends AbstractClientSizeWarning
{
    @BeforeClass
    public static void setupClass() throws IOException
    {
        AbstractClientSizeWarning.setupClass();

        // setup threshold after init to avoid driver issues loading
        // the test uses a rather small limit, which causes driver to fail while loading metadata
        CLUSTER.stream().forEach(i -> i.runOnInstance(() -> {
            DatabaseDescriptor.setCoordinatorReadSizeWarnThreshold(new DataStorageSpec.LongBytesBound(1, KIBIBYTES));
            DatabaseDescriptor.setCoordinatorReadSizeFailThreshold(new DataStorageSpec.LongBytesBound(2, KIBIBYTES));
        }));
    }

    private static void assertPrefix(String expectedPrefix, String actual)
    {
        if (!actual.startsWith(expectedPrefix))
            throw new AssertionError(String.format("expected \"%s\" to begin with \"%s\"", actual, expectedPrefix));
    }

    @Override
    protected void assertWarnings(List<String> warnings)
    {
        assertThat(warnings).hasSize(1);
        assertPrefix("Read on table " + KEYSPACE + ".tbl has exceeded the size warning threshold", warnings.get(0));
    }

    @Override
    protected void assertAbortWarnings(List<String> warnings)
    {
        assertThat(warnings).hasSize(1);
        assertPrefix("Read on table " + KEYSPACE + ".tbl has exceeded the size failure threshold", warnings.get(0));
    }

    @Override
    protected long[] getHistogram()
    {
        return CLUSTER.stream().mapToLong(i -> i.metrics().getCounter("org.apache.cassandra.metrics.keyspace.CoordinatorReadSize." + KEYSPACE)).toArray();
    }

    @Override
    protected long totalWarnings()
    {
        return CLUSTER.stream().mapToLong(i -> i.metrics().getCounter("org.apache.cassandra.metrics.keyspace.CoordinatorReadSizeWarnings." + KEYSPACE)).sum();
    }

    @Override
    protected long totalAborts()
    {
        return CLUSTER.stream().mapToLong(i -> i.metrics().getCounter("org.apache.cassandra.metrics.keyspace.CoordinatorReadSizeAborts." + KEYSPACE)).sum();
    }
}
