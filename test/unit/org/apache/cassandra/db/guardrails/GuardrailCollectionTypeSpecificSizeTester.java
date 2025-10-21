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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.function.Function;

import org.junit.After;
import org.apache.cassandra.config.DataStorageSpec;
import static org.apache.cassandra.config.DataStorageSpec.DataStorageUnit.BYTES;

/**
 * Tests the guardrail for the size of collections, {@link Guardrails#collectionSize}.
 * <p>
 * This test doesn't include the activation of the guardrail during sstable writes, these cases are covered by the dtest
 * {@link org.apache.cassandra.distributed.test.guardrails.GuardrailCollectionSizeOnSSTableWriteTest}.
 */
public abstract class GuardrailCollectionTypeSpecificSizeTester extends ThresholdTester
{
    protected static final int WARN_THRESHOLD = 1024; // bytes
    protected static final int FAIL_THRESHOLD = WARN_THRESHOLD * 4; // bytes

    public GuardrailCollectionTypeSpecificSizeTester(Threshold threshold,
                                                     TriConsumer<Guardrails, String, String> setter,
                                                     Function<Guardrails, String> warnGetter,
                                                     Function<Guardrails, String> failGetter)
    {
        super(WARN_THRESHOLD + "B",
              FAIL_THRESHOLD + "B",
              threshold,
              setter,
              warnGetter,
              failGetter,
              bytes -> new DataStorageSpec.LongBytesBound(bytes, BYTES).toString(),
              size -> new DataStorageSpec.LongBytesBound(size).toBytes());
    }

    @After
    public void after()
    {
        // immediately drop the created table so its async cleanup doesn't interfere with the next tests
        if (currentTable() != null)
            dropTable("DROP TABLE %s");
    }

    @Override
    protected String createTable(String query)
    {
        String table = super.createTable(query);
        disableCompaction();
        return table;
    }

    protected void assertValid(String query, ByteBuffer... values) throws Throwable
    {
        assertValid(execute(query, values));
    }

    protected void assertWarns(String query, ByteBuffer... values) throws Throwable
    {
        assertWarns(execute(query, values), "Detected collection v");
    }

    protected void assertFails(String query, ByteBuffer... values) throws Throwable
    {
        assertFails(execute(query, values), "Detected collection v");
    }

    protected CheckedFunction execute(String query, ByteBuffer... values)
    {
        return () -> execute(userClientState, query, Arrays.asList(values));
    }
}
