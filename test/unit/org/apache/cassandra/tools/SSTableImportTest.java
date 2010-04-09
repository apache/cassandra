/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.tools;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.filter.NamesQueryFilter;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.io.sstable.SSTableReader;
import static org.apache.cassandra.utils.FBUtilities.hexToBytes;
import static org.apache.cassandra.io.sstable.SSTableUtils.tempSSTableFile;
import org.json.simple.parser.ParseException;
import org.junit.Test;

public class SSTableImportTest extends SchemaLoader
{   
    @Test
    public void testImportSimpleCf() throws IOException, ParseException
    {
        // Import JSON to temp SSTable file
        String jsonUrl = getClass().getClassLoader().getResource("SimpleCF.json").getPath();
        File tempSS = tempSSTableFile("Keyspace1", "Standard1");
        SSTableImport.importJson(jsonUrl, "Keyspace1", "Standard1", tempSS.getPath());

        // Verify results
        SSTableReader reader = SSTableReader.open(tempSS.getPath(), DatabaseDescriptor.getPartitioner());
        QueryFilter qf = QueryFilter.getNamesFilter("rowA", new QueryPath("Standard1", null, null), "colAA".getBytes());
        ColumnFamily cf = qf.getSSTableColumnIterator(reader).getColumnFamily();
        assert Arrays.equals(cf.getColumn("colAA".getBytes()).value(), hexToBytes("76616c4141"));
    }

    @Test
    public void testImportSuperCf() throws IOException, ParseException
    {
        String jsonUrl = getClass().getClassLoader().getResource("SuperCF.json").getPath();
        File tempSS = tempSSTableFile("Keyspace1", "Super4");
        SSTableImport.importJson(jsonUrl, "Keyspace1", "Super4", tempSS.getPath());
        
        // Verify results
        SSTableReader reader = SSTableReader.open(tempSS.getPath(), DatabaseDescriptor.getPartitioner());
        QueryFilter qf = QueryFilter.getNamesFilter("rowA", new QueryPath("Super4", null, null), "superA".getBytes());
        ColumnFamily cf = qf.getSSTableColumnIterator(reader).getColumnFamily();
        IColumn superCol = cf.getColumn("superA".getBytes());
        assert Arrays.equals(superCol.getSubColumn("colAA".getBytes()).value(), hexToBytes("76616c75654141"));
    }
}
