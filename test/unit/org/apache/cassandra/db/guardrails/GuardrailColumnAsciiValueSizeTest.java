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

import org.apache.cassandra.config.DataStorageSpec;
import static org.apache.cassandra.config.DataStorageSpec.DataStorageUnit.BYTES;

/**
 * Tests the guardrail for the size of ascii column values, {@link Guardrails#columnAsciiValueSize}.
 */
public class GuardrailColumnAsciiValueSizeTest extends ColumnTypeSpecificValueThresholdTester
{
    private static final int WARN_THRESHOLD = 1024; // bytes
    private static final int FAIL_THRESHOLD = WARN_THRESHOLD * 4; // bytes

    public GuardrailColumnAsciiValueSizeTest()
    {
        super(WARN_THRESHOLD + "B",
              FAIL_THRESHOLD + "B",
              Guardrails.columnAsciiValueSize,
              Guardrails::setColumnAsciiValueSizeThreshold,
              Guardrails::getColumnAsciiValueSizeWarnThreshold,
              Guardrails::getColumnAsciiValueSizeFailThreshold,
              bytes -> new DataStorageSpec.LongBytesBound(bytes, BYTES).toString(),
              size -> new DataStorageSpec.LongBytesBound(size).toBytes());
    }

    @Override
    protected int warnThreshold()
    {
        return WARN_THRESHOLD;
    }

    @Override
    protected int failThreshold()
    {
        return FAIL_THRESHOLD;
    }

    @Override
    protected String columnType()
    {
        return "ascii";
    }
}
