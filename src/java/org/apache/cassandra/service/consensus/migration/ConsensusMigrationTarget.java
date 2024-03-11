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

import org.apache.cassandra.service.consensus.TransactionalMode;

public enum ConsensusMigrationTarget
{
    paxos(0),
    accord(1);

    public final byte value;

    ConsensusMigrationTarget(int value)
    {
        this.value = SignedBytes.checkedCast(value);
    }

    public static ConsensusMigrationTarget fromString(String targetProtocol)
    {
        return ConsensusMigrationTarget.valueOf(targetProtocol.toLowerCase());
    }

    public static ConsensusMigrationTarget fromValue(byte value)
    {
        switch (value)
        {
            default:
                throw new IllegalArgumentException(value + " is not recognized");
            case 0:
                return paxos;
            case 1:
                return accord;
        }
    }

    public static ConsensusMigrationTarget fromTransactionalMode(TransactionalMode mode)
    {
        return mode.accordIsEnabled ? accord : paxos;
    }
}
