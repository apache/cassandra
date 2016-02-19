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

package org.apache.cassandra.io.compress;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.exceptions.ConfigurationException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CQLCompressionTest extends CQLTester
{
    @Test
    public void lz4ParamsTest()
    {
        createTable("create table %s (id int primary key, uh text) with compression = {'class':'LZ4Compressor', 'lz4_high_compressor_level':3}");
        assertTrue(((LZ4Compressor)getCurrentColumnFamilyStore().metadata.params.compression.getSstableCompressor()).compressorType.equals(LZ4Compressor.LZ4_FAST_COMPRESSOR));
        createTable("create table %s (id int primary key, uh text) with compression = {'class':'LZ4Compressor', 'lz4_compressor_type':'high', 'lz4_high_compressor_level':13}");
        assertEquals(((LZ4Compressor)getCurrentColumnFamilyStore().metadata.params.compression.getSstableCompressor()).compressorType, LZ4Compressor.LZ4_HIGH_COMPRESSOR);
        assertEquals(((LZ4Compressor)getCurrentColumnFamilyStore().metadata.params.compression.getSstableCompressor()).compressionLevel, (Integer)13);
        createTable("create table %s (id int primary key, uh text) with compression = {'class':'LZ4Compressor'}");
        assertEquals(((LZ4Compressor)getCurrentColumnFamilyStore().metadata.params.compression.getSstableCompressor()).compressorType, LZ4Compressor.LZ4_FAST_COMPRESSOR);
        assertEquals(((LZ4Compressor)getCurrentColumnFamilyStore().metadata.params.compression.getSstableCompressor()).compressionLevel, (Integer)9);
    }

    @Test(expected = ConfigurationException.class)
    public void lz4BadParamsTest() throws Throwable
    {
        try
        {
            createTable("create table %s (id int primary key, uh text) with compression = {'class':'LZ4Compressor', 'lz4_compressor_type':'high', 'lz4_high_compressor_level':113}");
        }
        catch (RuntimeException e)
        {
            throw e.getCause();
        }
    }
}
