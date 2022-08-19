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

package org.apache.cassandra.cql3.functions.masking;

import org.apache.cassandra.cql3.CQL3Type;

import static java.lang.String.format;

/**
 * Tests for {@link DefaultMaskingFunction}.
 */
public class DefaultMaskingFunctionTest extends MaskingFunctionTester
{
    @Override
    protected void testMaskingOnColumn(String name, CQL3Type type, Object value) throws Throwable
    {
        assertRows(execute(format("SELECT mask_default(%s) FROM %%s", name)),
                   row(type.getType().getMaskedValue()));
    }
}
