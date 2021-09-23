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

package org.apache.cassandra.service.reads;

import org.junit.Test;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.service.QueryInfoTracker;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public class ReadTrackingTransformationTest
{
    @Test
    public void testOnRowHandling() throws Throwable
    {
        QueryInfoTracker.ReadTracker readTracker = mock(QueryInfoTracker.ReadTracker.class);
        Row row = mock(Row.class);

        ReadTrackingTransformation transformation = new ReadTrackingTransformation(readTracker);
        transformation.applyToRow(row);
        Mockito.verify(readTracker).onRow(Mockito.eq(row));

        doThrow(RuntimeException.class).when(readTracker).onRow(any(Row.class));
        transformation.applyToRow(row); // swallows exception
    }

    @Test
    public void testOnPartitionHandling() throws Throwable
    {
        QueryInfoTracker.ReadTracker readTracker = mock(QueryInfoTracker.ReadTracker.class);
        DecoratedKey key = mock(DecoratedKey.class);

        ReadTrackingTransformation transformation = new ReadTrackingTransformation(readTracker);
        transformation.applyToPartitionKey(key);
        Mockito.verify(readTracker).onPartition(Mockito.eq(key));

        doThrow(RuntimeException.class).when(readTracker).onPartition(any(DecoratedKey.class));
        transformation.applyToPartitionKey(key); // swallows exception
    }
}