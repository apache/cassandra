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

import org.junit.Test;

import static org.junit.Assert.*;

public class OperationTypeTest
{
    @Test
    public void stuffNeededByCNDBShouldNotBeRemovedFromTheCodebase()
    {
        // this "test" is made solely to explicitly reference elements used
        // by cndb (and [likely] unused in Cassandra)
        assertEquals(OperationType.RESTORE.toString(), OperationType.RESTORE.type);
        assertEquals(OperationType.REMOTE_RELOAD.toString(), OperationType.REMOTE_RELOAD.type);
        assertEquals(OperationType.REMOTE_COMPACTION.toString(), OperationType.REMOTE_COMPACTION.type);
        assertEquals(OperationType.TRUNCATE_TABLE.toString(), OperationType.TRUNCATE_TABLE.type);
        assertEquals(OperationType.DROP_TABLE.toString(), OperationType.DROP_TABLE.type);
        assertEquals(OperationType.REMOVE_UNREADEABLE.toString(), OperationType.REMOVE_UNREADEABLE.type);
        assertEquals(OperationType.REGION_BOOTSTRAP.toString(), OperationType.REGION_BOOTSTRAP.type);
        assertEquals(OperationType.REGION_DECOMMISSION.toString(), OperationType.REGION_DECOMMISSION.type);
        assertEquals(OperationType.REGION_REPAIR.toString(), OperationType.REGION_REPAIR.type);
        assertEquals(OperationType.SSTABLE_DISCARD.toString(), OperationType.SSTABLE_DISCARD.type);
        assertTrue(OperationType.SSTABLE_DISCARD.localOnly);
        assertTrue(OperationType.KEY_CACHE_SAVE.isCacheSave());
        assertFalse(OperationType.EXCEPT_VALIDATIONS.test(OperationType.VALIDATION));
        assertTrue(OperationType.COMPACTIONS_ONLY.test(OperationType.COMPACTION));
        assertTrue(OperationType.REWRITES_SSTABLES.test(OperationType.UPGRADE_SSTABLES));
    }
}