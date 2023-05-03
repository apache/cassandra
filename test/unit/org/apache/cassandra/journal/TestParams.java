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
package org.apache.cassandra.journal;

import org.apache.cassandra.net.MessagingService;

public class TestParams implements Params
{
    static final TestParams INSTANCE = new TestParams();

    @Override
    public int segmentSize()
    {
        return 32 << 20;
    }

    @Override
    public FailurePolicy failurePolicy()
    {
        return FailurePolicy.STOP;
    }

    @Override
    public FlushMode flushMode()
    {
        return FlushMode.GROUP;
    }

    @Override
    public int flushPeriod()
    {
        return 1000;
    }

    @Override
    public int periodicFlushLagBlock()
    {
        return 1500;
    }

    @Override
    public int userVersion()
    {
        return MessagingService.current_version;
    }
}
