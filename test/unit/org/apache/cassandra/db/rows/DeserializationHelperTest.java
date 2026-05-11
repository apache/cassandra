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

package org.apache.cassandra.db.rows;


import java.io.DataInput;

import org.junit.Test;

import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.io.util.TrackedDataInputPlus;
import org.apache.cassandra.schema.TableMetadata;

import static org.apache.cassandra.net.MessagingService.Version.VERSION_60;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;

public class DeserializationHelperTest
{
    static TableMetadata metadata =
    TableMetadata.builder("DeserializationHelperTest", "Test")
                 .addPartitionKeyColumn("key", AsciiType.instance)
                 .addClusteringColumn("clustering", Int32Type.instance)
                 .addRegularColumn("data", Int32Type.instance)
                 .build();

    @Test
    public void testTrackedDataInputPlusIsReusable()
    {
        DeserializationHelper helper = new DeserializationHelper(metadata, VERSION_60.value, DeserializationHelper.Flag.LOCAL);
        TrackedDataInputPlus trackedDataInputPlus = helper.trackedDataInputPlus(mock(DataInput.class), 0);
        assertSame(trackedDataInputPlus, helper.trackedDataInputPlus(mock(DataInput.class), 1));
    }
}
