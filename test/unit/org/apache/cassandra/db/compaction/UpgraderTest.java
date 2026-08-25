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

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SequenceBasedSSTableId;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.format.bti.BtiFormat;
import org.apache.cassandra.io.util.File;

import static org.assertj.core.api.Assertions.assertThat;

public class UpgraderTest
{
    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testLatestVersionsAreCurrent()
    {
        SSTableFormat<?, ?> big = BigFormat.getInstance();
        Assume.assumeTrue(big.getLatestVersion().version.equals("pa"));

        assertThat(Upgrader.isCurrentVersion(descriptor(big.getVersion("pa")))).isTrue();
        assertThat(Upgrader.isCurrentVersion(descriptor(big.getVersion("pb")))).isTrue();
    }

    @Test
    public void testSelectedFormatRequiredForStandaloneUpgrade()
    {
        SSTableFormat<?, ?> big = BigFormat.getInstance();
        SSTableFormat<?, ?> bti = DatabaseDescriptor.getSSTableFormats().get(BtiFormat.NAME);
        Assume.assumeTrue(big.getLatestVersion().version.equals("pa"));

        Descriptor pb = descriptor(big.getVersion("pb"));
        Descriptor latestBti = descriptor(bti.getLatestVersion());

        assertThat(Upgrader.isCurrentVersion(pb)).isTrue();
        assertThat(Upgrader.isCurrentVersion(pb, bti)).isFalse();
        assertThat(Upgrader.isCurrentVersion(latestBti)).isTrue();
        assertThat(Upgrader.isCurrentVersion(latestBti, big)).isFalse();
    }

    private static Descriptor descriptor(Version version)
    {
        return new Descriptor(version, new File("unused"), "ks", "cf", new SequenceBasedSSTableId(1));
    }
}
