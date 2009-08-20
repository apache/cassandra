/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.db;

import java.io.IOException;

import org.junit.Test;
import org.junit.Assert;

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.service.StorageService;

public class SystemTableTest extends CleanupHelper
{

    @Test
    public void testOnlyOnceCreationOfStorageMetadata() throws IOException
    {
        SystemTable.StorageMetadata storageMetadata1 = SystemTable.initMetadata();
        SystemTable.StorageMetadata storageMetadata2 = SystemTable.initMetadata();
        Assert.assertTrue("smd should not change after calling initMetadata twice", storageMetadata1 == storageMetadata2);
    }

    @Test
    public void testTokenGetsUpdated() throws IOException
    {
        SystemTable.StorageMetadata storageMetadata1 = SystemTable.initMetadata();
        SystemTable.updateToken(StorageService.getPartitioner().getToken("503545744:0"));
        SystemTable.StorageMetadata storageMetadata2 = SystemTable.initMetadata();
        Assert.assertTrue("smd should still be a singleton after updateToken", storageMetadata1 == storageMetadata2);
    }
}
