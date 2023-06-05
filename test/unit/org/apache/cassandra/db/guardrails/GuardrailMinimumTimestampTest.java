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

package org.apache.cassandra.db.guardrails;

import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.service.ClientState;

public class GuardrailMinimumTimestampTest extends ThresholdTester
{
    public GuardrailMinimumTimestampTest()
    {
        super(TimeUnit.DAYS.toSeconds(1) + "s",
              TimeUnit.DAYS.toSeconds(2) + "s",
              Guardrails.minimumAllowableTimestamp,
              Guardrails::setMinimumTimestampThreshold,
              Guardrails::getMinimumTimestampWarnThreshold,
              Guardrails::getMinimumTimestampFailThreshold,
              micros -> new DurationSpec.LongMicrosecondsBound(micros, TimeUnit.MICROSECONDS).toString(),
              micros -> new DurationSpec.LongMicrosecondsBound(micros).toMicroseconds());
    }

    @Before
    public void setupTest()
    {
        createTable("CREATE TABLE IF NOT EXISTS %s (k int primary key, v int)");
    }

    @Test
    public void testDisabled() throws Throwable
    {
        guardrails().setMinimumTimestampThreshold(null, null);
        assertValid("INSERT INTO %s (k, v) VALUES (1, 1) USING TIMESTAMP 12345");
    }

    @Test
    public void testEnabledFailure() throws Throwable
    {
        assertFails("INSERT INTO %s (k, v) VALUES (1, 1) USING TIMESTAMP 12345", "minimum_timestamp violated");
    }

    @Test
    public void testEnabledInRange() throws Throwable
    {
        assertValid("INSERT INTO %s (k, v) VALUES (1, 1) USING TIMESTAMP " + ClientState.getTimestamp());
    }

    @Test
    public void testEnabledWarn() throws Throwable
    {
        assertWarns("INSERT INTO %s (k, v) VALUES (1, 1) USING TIMESTAMP " +  (ClientState.getTimestamp() - (TimeUnit.DAYS.toMicros(1) + 40000)),
                    "minimum_timestamp violated");
    }
}
