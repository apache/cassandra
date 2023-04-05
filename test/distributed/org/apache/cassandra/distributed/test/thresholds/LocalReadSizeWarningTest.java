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

import org.junit.BeforeClass;

import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.config.DatabaseDescriptor;

import static org.apache.cassandra.config.DataStorageSpec.DataStorageUnit.KIBIBYTES;
import static org.assertj.core.api.Assertions.assertThat;

public class LocalReadSizeWarningTest extends AbstractClientSizeWarning
{
    @BeforeClass
    public static void setupClass() throws IOException
    {
        AbstractClientSizeWarning.setupClass();

        // setup threshold after init to avoid driver issues loading
        // the test uses a rather small limit, which causes driver to fail while loading metadata
        CLUSTER.stream().forEach(i -> i.runOnInstance(() -> {
            // disable coordinator version
            DatabaseDescriptor.setCoordinatorReadSizeWarnThreshold(null);
            DatabaseDescriptor.setCoordinatorReadSizeFailThreshold(null);

            DatabaseDescriptor.setLocalReadSizeWarnThreshold(new DataStorageSpec.LongBytesBound(1, KIBIBYTES));
            DatabaseDescriptor.setLocalReadSizeFailThreshold(new DataStorageSpec.LongBytesBound(2, KIBIBYTES));
        }));
    }

    @Override
    protected void assertWarnings(List<String> warnings)
    {
        assertThat(warnings).hasSize(1);
        assertThat(warnings.get(0)).contains("(see local_read_size_warn_threshold)").contains("and issued local read size warnings for query");
    }

    @Override
    protected void assertAbortWarnings(List<String> warnings)
    {
        assertThat(warnings).hasSize(1);
        assertThat(warnings.get(0)).contains("(see local_read_size_fail_threshold)").contains("aborted the query");
    }

    @Override
    protected long[] getHistogram()
    {
        return CLUSTER.stream().mapToLong(i -> i.metrics().getCounter("org.apache.cassandra.metrics.keyspace.LocalReadSize." + KEYSPACE)).toArray();
    }

    @Override
    protected long totalWarnings()
    {
        return CLUSTER.stream().mapToLong(i -> i.metrics().getCounter("org.apache.cassandra.metrics.keyspace.LocalReadSizeWarnings." + KEYSPACE)).sum();
    }

    @Override
    protected long totalAborts()
    {
        return CLUSTER.stream().mapToLong(i -> i.metrics().getCounter("org.apache.cassandra.metrics.keyspace.LocalReadSizeAborts." + KEYSPACE)).sum();
    }
}
