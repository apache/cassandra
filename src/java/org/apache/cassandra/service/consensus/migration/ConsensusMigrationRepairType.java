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

package org.apache.cassandra.service.consensus.migration;

import com.google.common.primitives.SignedBytes;

public enum ConsensusMigrationRepairType
{
    ineligible(0),
    paxos(1),
    accord(2),
    either(3);

    public final byte value;

    ConsensusMigrationRepairType(int value)
    {
        this.value = SignedBytes.checkedCast(value);
    }

    public static ConsensusMigrationRepairType fromString(String repairType)
    {
        return ConsensusMigrationRepairType.valueOf(repairType.toLowerCase());
    }

    public static ConsensusMigrationRepairType fromValue(byte value)
    {
        switch (value)
        {
            default:
                throw new IllegalArgumentException(value + " is not recognized");
            case 0:
                return ConsensusMigrationRepairType.ineligible;
            case 1:
                return ConsensusMigrationRepairType.paxos;
            case 2:
                return ConsensusMigrationRepairType.accord;
            case 3:
                return ConsensusMigrationRepairType.either;
        }
    }
}
