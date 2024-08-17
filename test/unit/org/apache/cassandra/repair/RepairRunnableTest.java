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

package org.apache.cassandra.repair;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.PreviewKind;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.when;


public class RepairRunnableTest extends CQLTester
{
    @Mock
    StorageService storageService;

    @Mock
    RepairOption repairOption;

    @Mock
    Throwable throwable;

    @Before
    public void setUp()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testFailNPE()
    {
        RepairRunnable repairRunnable = new RepairRunnable(storageService, 0, repairOption, KEYSPACE);
        repairRunnable.firstError.set(throwable);
        when(throwable.getMessage()).thenReturn(null);
        when(repairOption.getPreviewKind()).thenReturn(PreviewKind.NONE);

        repairRunnable.fail(null);
    }
}
