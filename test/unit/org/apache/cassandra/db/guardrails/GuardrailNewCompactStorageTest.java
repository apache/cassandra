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

import org.junit.Test;

public class GuardrailNewCompactStorageTest extends GuardrailTester
{
    private void setGuardrail(boolean enabled)
    {
        Guardrails.instance.setCompactTablesEnabled(enabled);
    }

    @Test
    public void testFeatureEnabled() throws Throwable
    {
        setGuardrail(true);
        assertValid("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH COMPACT STORAGE");
    }

    @Test
    public void testFeatureDisabled() throws Throwable
    {
        setGuardrail(false);
        assertFails("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH COMPACT STORAGE",
                    "Creation of new COMPACT STORAGE tables");
    }
}
