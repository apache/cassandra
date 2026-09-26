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

package org.apache.cassandra.db.compaction.differential;


/** Runs the inherited allocation tests with the BTI format selected. */
public class BtiCursorCompactionAllocationGateTest extends CursorCompactionAllocationGateTest
{
    @Override
    protected String formatName()
    {
        return "bti";
    }

    @Override
    protected long ceilingBytes()
    {
        return 768 * 1024;
    }

    /** Range-tombstone allocation ceiling per input byte under BTI. */
    @Override
    protected double rtPerInputByteCeiling()
    {
        return 1.3;
    }

    /** Complex-column allocation ceiling per input byte under BTI. */
    @Override
    protected double complexPerInputByteCeiling()
    {
        return 0.6;
    }

    /** Large-file allocation ceiling per input byte under BTI. */
    @Override
    protected double largeFilePerInputByteCeiling()
    {
        return 0.32;
    }
}
