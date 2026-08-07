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

package org.apache.cassandra.db;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * CASSANDRA-21356: ReusableLivenessInfo.isExpiring() checked {@code localExpirationTime !=
 * NO_EXPIRATION_TIME} instead of {@code ttl != NO_TTL}. A tombstone cell also has a non-default
 * localExpirationTime (it stores the deletion timestamp there), so the old check returned true
 * for tombstones as well as expiring cells — violating the LivenessInfo contract that
 * IS_DELETED_MASK and IS_EXPIRING_MASK are mutually exclusive, and matching the canonical
 * definition in AbstractCell.isExpiring() (ttl() != NO_TTL).
 */
public class ReusableLivenessInfoTest
{
    @Test
    public void tombstoneIsNotExpiring()
    {
        ReusableLivenessInfo info = new ReusableLivenessInfo();
        // A tombstone cell (e.g. from INSERT ... null or DELETE): ttl is NO_TTL, but
        // localExpirationTime is still set — it stores the deletion timestamp.
        info.reset(1L, LivenessInfo.NO_TTL, 12345L);
        assertTrue("ttl=NO_TTL with a set localExpirationTime is a tombstone", info.isTombstone());
        assertFalse("isExpiring() must not fire for a tombstone cell: it and IS_DELETED_MASK " +
                    "are mutually exclusive in the SSTable format",
                    info.isExpiring());
    }

    @Test
    public void expiringCellIsExpiringNotTombstone()
    {
        ReusableLivenessInfo info = new ReusableLivenessInfo();
        info.reset(1L, 3600, 12345L);
        assertTrue("A cell with a positive ttl is expiring", info.isExpiring());
        assertFalse("An expiring cell is not a tombstone", info.isTombstone());
    }
}
