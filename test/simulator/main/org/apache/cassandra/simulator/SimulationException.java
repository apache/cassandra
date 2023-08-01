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

package org.apache.cassandra.simulator;

public class SimulationException extends RuntimeException
{
    public SimulationException(long seed, Throwable t)
    {
        super(createMsg(seed, null), t, true, false);
    }

    public SimulationException(long seed, String msg, Throwable t)
    {
        super(createMsg(seed, msg), t, true, false);
    }

    private static String createMsg(long seed, String msg)
    {
        return String.format("Failed on seed 0x%s%s", Long.toHexString(seed), msg == null ? "" : "; " + msg);
    }
}
