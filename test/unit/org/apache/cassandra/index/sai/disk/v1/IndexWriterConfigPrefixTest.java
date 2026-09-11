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
package org.apache.cassandra.index.sai.disk.v1;

import java.util.Collections;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.index.sai.utils.IndexTermType;
import org.apache.cassandra.schema.ColumnMetadata;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IndexWriterConfigPrefixTest
{
    private static IndexTermType literalType()
    {
        ColumnMetadata col = ColumnMetadata.regularColumn("ks", "t", "v", UTF8Type.instance, ColumnMetadata.NO_UNIQUE_ID);
        return IndexTermType.create(col, Collections.emptyList(), IndexTarget.Type.SIMPLE);
    }

    @Test
    public void testDisabledByDefault()
    {
        IndexWriterConfig cfg = IndexWriterConfig.fromOptions(null, literalType(), Map.of());
        assertFalse(cfg.isLiteralPrefixEnabled());
    }

    @Test
    public void testEnabledWhenSet()
    {
        IndexWriterConfig cfg = IndexWriterConfig.fromOptions(
            null, literalType(),
            Map.of(IndexWriterConfig.ENABLE_LITERAL_PREFIX_SAI, "true"));
        assertTrue(cfg.isLiteralPrefixEnabled());
    }
}
