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

package org.apache.cassandra.io.util;

import java.util.zip.Checksum;

public interface Checksumed
{
    Checksum checksum();

    default long getAndResetChecksum()
    {
        Checksum c = checksum();
        long v = c.getValue();
        c.reset();
        return v;
    }

    default int getValue32()
    {
        long v = checksum().getValue();
        if (Long.numberOfLeadingZeros(v) < 32)
            throw new IllegalStateException("Checksum is larger than 32 bytes!");
        return (int) v;
    }

    default int getValue32AndResetChecksum()
    {
        Checksum c = checksum();
        long v = c.getValue();
        if (Long.numberOfLeadingZeros(v) < 32)
            throw new IllegalStateException("Checksum is larger than 32 bytes!");
        c.reset();
        return (int) v;
    }

    default void resetChecksum()
    {
        checksum().reset();
    }
}
