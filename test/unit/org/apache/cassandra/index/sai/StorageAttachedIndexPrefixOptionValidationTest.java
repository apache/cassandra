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
package org.apache.cassandra.index.sai;

import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.disk.v1.IndexWriterConfig;
import org.apache.cassandra.schema.TableMetadata;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class StorageAttachedIndexPrefixOptionValidationTest
{
    @Test
    public void shouldAllowLiteralPrefixOptionOnLiteralColumn()
    {
        Map<String, String> options = options("val", "true");
        assertTrue(StorageAttachedIndex.validateOptions(options, textTable()).isEmpty());
    }

    @Test
    public void shouldRejectLiteralPrefixOptionOnNonLiteralColumn()
    {
        try
        {
            StorageAttachedIndex.validateOptions(options("val", "true"), intTable());
            fail("Expected InvalidRequestException");
        }
        catch (InvalidRequestException e)
        {
            assertTrue(e.getMessage().contains(IndexWriterConfig.ENABLE_LITERAL_PREFIX_SAI + " is only supported on string/literal columns"));
        }
    }

    @Test
    public void shouldRejectInvalidLiteralPrefixOptionValue()
    {
        try
        {
            StorageAttachedIndex.validateOptions(options("val", "maybe"), textTable());
            fail("Expected InvalidRequestException");
        }
        catch (InvalidRequestException e)
        {
            assertTrue(e.getMessage().contains(IndexWriterConfig.ENABLE_LITERAL_PREFIX_SAI + " must be 'true' or 'false'"));
        }
    }

    private static Map<String, String> options(String target, String value)
    {
        return Map.of(IndexTarget.CUSTOM_INDEX_OPTION_NAME, StorageAttachedIndex.class.getCanonicalName(),
                      IndexTarget.TARGET_OPTION_NAME, target,
                      IndexWriterConfig.ENABLE_LITERAL_PREFIX_SAI, value);
    }

    private static TableMetadata textTable()
    {
        return TableMetadata.builder("ks", "tbl")
                            .addPartitionKeyColumn("pk", Int32Type.instance)
                            .addRegularColumn("val", UTF8Type.instance)
                            .partitioner(Murmur3Partitioner.instance)
                            .build();
    }

    private static TableMetadata intTable()
    {
        return TableMetadata.builder("ks", "tbl")
                            .addPartitionKeyColumn("pk", Int32Type.instance)
                            .addRegularColumn("val", Int32Type.instance)
                            .partitioner(Murmur3Partitioner.instance)
                            .build();
    }
}
