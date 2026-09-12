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

package org.apache.cassandra.service;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The {@code FlushCompression} attribute of the StorageService MBean, which makes the node-wide
 * flush_compression setting changeable at runtime without a restart.
 */
public class StorageServiceFlushCompressionTest
{
    private static Config.FlushCompression defaultFlush;

    @BeforeClass
    public static void setup()
    {
        ServerTestUtils.daemonInitialization();
        defaultFlush = DatabaseDescriptor.getFlushCompression();
    }

    @After
    public void restoreFlushCompression()
    {
        DatabaseDescriptor.setFlushCompression(defaultFlush);
    }

    @Test
    public void everyValueRoundTrips()
    {
        for (Config.FlushCompression value : Config.FlushCompression.values())
        {
            StorageService.instance.setFlushCompression(value.toString());
            assertThat(StorageService.instance.getFlushCompression()).isEqualTo(value.toString());
            assertThat(DatabaseDescriptor.getFlushCompression()).isEqualTo(value);
        }
    }

    @Test
    public void getterReflectsDatabaseDescriptor()
    {
        DatabaseDescriptor.setFlushCompression(Config.FlushCompression.table);
        assertThat(StorageService.instance.getFlushCompression()).isEqualTo("table");
    }

    @Test
    public void unknownValueIsRejectedAndPreviousIsKept()
    {
        StorageService.instance.setFlushCompression(Config.FlushCompression.table.toString());

        assertThatThrownBy(() -> StorageService.instance.setFlushCompression("bogus"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid flush_compression: bogus")
        .hasMessageContaining("[none, fast, table]");

        assertThat(DatabaseDescriptor.getFlushCompression()).isEqualTo(Config.FlushCompression.table);
    }

    @Test
    public void valuesAreCaseSensitive()
    {
        assertThatThrownBy(() -> StorageService.instance.setFlushCompression("NONE"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid flush_compression: NONE");
    }

    @Test
    public void nullIsRejected()
    {
        assertThatThrownBy(() -> StorageService.instance.setFlushCompression(null))
        .isInstanceOf(IllegalArgumentException.class);
    }
}
