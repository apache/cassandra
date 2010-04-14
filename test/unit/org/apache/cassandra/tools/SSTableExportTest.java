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
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.filter.NamesQueryFilter;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableWriter;
import org.apache.cassandra.io.util.DataOutputBuffer;
import static org.apache.cassandra.io.sstable.SSTableUtils.tempSSTableFile;
import static org.apache.cassandra.utils.FBUtilities.hexToBytes;
import static org.junit.Assert.assertTrue;

import org.apache.cassandra.Util;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.ParseException;
import org.junit.Test;

public class SSTableExportTest extends SchemaLoader
{
    @Test
    public void testEnumeratekeys() throws IOException
    {
        File tempSS = tempSSTableFile("Keyspace1", "Standard1");
        ColumnFamily cfamily = ColumnFamily.create("Keyspace1", "Standard1");
        IPartitioner<?> partitioner = DatabaseDescriptor.getPartitioner();
        DataOutputBuffer dob = new DataOutputBuffer();
        SSTableWriter writer = new SSTableWriter(tempSS.getPath(), 2, partitioner);
        
        // Add rowA
        cfamily.addColumn(new QueryPath("Standard1", null, "colA".getBytes()), "valA".getBytes(), 1, false);
        ColumnFamily.serializer().serializeWithIndexes(cfamily, dob);
        writer.append(Util.dk("rowA"), dob);
        dob.reset();
        cfamily.clear();
        
        // Add rowB
        cfamily.addColumn(new QueryPath("Standard1", null, "colB".getBytes()), "valB".getBytes(), 1, false);
        ColumnFamily.serializer().serializeWithIndexes(cfamily, dob);
        writer.append(Util.dk("rowB"), dob);
        dob.reset();
        cfamily.clear();
     
        writer.closeAndOpenReader();
        
        // Enumerate and verify
        File temp = File.createTempFile("Standard1", ".txt");
        SSTableExport.enumeratekeys(writer.getFilename(), new PrintStream(temp.getPath()));

        
        FileReader file = new FileReader(temp);
        char[] buf = new char[(int) temp.length()];
        file.read(buf);
        String output = new String(buf);

        String sep = System.getProperty("line.separator");
        // FIXME: string keys
        assert output.equals("rowA" + sep + "rowB" + sep) : output;
    }

    @Test
    public void testExportSimpleCf() throws IOException    {
        File tempSS = tempSSTableFile("Keyspace1", "Standard1");
        ColumnFamily cfamily = ColumnFamily.create("Keyspace1", "Standard1");
        IPartitioner<?> partitioner = DatabaseDescriptor.getPartitioner();
        DataOutputBuffer dob = new DataOutputBuffer();
        SSTableWriter writer = new SSTableWriter(tempSS.getPath(), 2, partitioner);
        
        // Add rowA
        cfamily.addColumn(new QueryPath("Standard1", null, "colA".getBytes()), "valA".getBytes(), 1, false);
        ColumnFamily.serializer().serializeWithIndexes(cfamily, dob);
        writer.append(Util.dk("rowA"), dob);
        dob.reset();
        cfamily.clear();
        
        // Add rowB
        cfamily.addColumn(new QueryPath("Standard1", null, "colB".getBytes()), "valB".getBytes(), 1, false);
        ColumnFamily.serializer().serializeWithIndexes(cfamily, dob);
        writer.append(Util.dk("rowB"), dob);
        dob.reset();
        cfamily.clear();

        // Add rowExclude
        cfamily.addColumn(new QueryPath("Standard1", null, "colX".getBytes()), "valX".getBytes(), 1, false);
        ColumnFamily.serializer().serializeWithIndexes(cfamily, dob);
        writer.append(Util.dk("rowExclude"), dob);
        dob.reset();
        cfamily.clear();

        SSTableReader reader = writer.closeAndOpenReader();
        
        // Export to JSON and verify
        File tempJson = File.createTempFile("Standard1", ".json");
        SSTableExport.export(reader, new PrintStream(tempJson.getPath()), new String[]{"rowExclude"});
        
        JSONObject json = (JSONObject)JSONValue.parse(new FileReader(tempJson));
        
        // FIXME: string keys
        JSONArray rowA = (JSONArray)json.get("rowA");
        JSONArray colA = (JSONArray)rowA.get(0);
        assert Arrays.equals(hexToBytes((String)colA.get(1)), "valA".getBytes());
        
        JSONArray rowB = (JSONArray)json.get("rowB");
        JSONArray colB = (JSONArray)rowB.get(0);
        assert !(Boolean)colB.get(3);

        JSONArray rowExclude = (JSONArray)json.get("rowExclude");
        assert rowExclude == null;
    }

    @Test
    public void testExportSuperCf() throws IOException
    {
        File tempSS = tempSSTableFile("Keyspace1", "Super4");
        ColumnFamily cfamily = ColumnFamily.create("Keyspace1", "Super4");
        IPartitioner<?> partitioner = DatabaseDescriptor.getPartitioner();
        DataOutputBuffer dob = new DataOutputBuffer();
        SSTableWriter writer = new SSTableWriter(tempSS.getPath(), 2, partitioner);
        
        // Add rowA
        cfamily.addColumn(new QueryPath("Super4", "superA".getBytes(), "colA".getBytes()), "valA".getBytes(), 1, false);
        ColumnFamily.serializer().serializeWithIndexes(cfamily, dob);
        writer.append(Util.dk("rowA"), dob);
        dob.reset();
        cfamily.clear();
        
        // Add rowB
        cfamily.addColumn(new QueryPath("Super4", "superB".getBytes(), "colB".getBytes()), "valB".getBytes(), 1, false);
        ColumnFamily.serializer().serializeWithIndexes(cfamily, dob);
        writer.append(Util.dk("rowB"), dob);
        dob.reset();
        cfamily.clear();

        // Add rowExclude
        cfamily.addColumn(new QueryPath("Super4", "superX".getBytes(), "colX".getBytes()), "valX".getBytes(), 1, false);
        ColumnFamily.serializer().serializeWithIndexes(cfamily, dob);
        writer.append(Util.dk("rowExclude"), dob);
        dob.reset();
        cfamily.clear();

        SSTableReader reader = writer.closeAndOpenReader();
        
        // Export to JSON and verify
        File tempJson = File.createTempFile("Super4", ".json");
        SSTableExport.export(reader, new PrintStream(tempJson.getPath()), new String[]{"rowExclude"});
        
        JSONObject json = (JSONObject)JSONValue.parse(new FileReader(tempJson));
        
        JSONObject rowA = (JSONObject)json.get("rowA");
        JSONObject superA = (JSONObject)rowA.get(cfamily.getComparator().getString("superA".getBytes()));
        JSONArray subColumns = (JSONArray)superA.get("subColumns");
        JSONArray colA = (JSONArray)subColumns.get(0);
        JSONObject rowExclude = (JSONObject)json.get("rowExclude");
        assert Arrays.equals(hexToBytes((String)colA.get(1)), "valA".getBytes());
        assert !(Boolean)colA.get(3);
        assert rowExclude == null;
    }
    
    @Test
    public void testRoundTripStandardCf() throws IOException, ParseException
    {
        File tempSS = tempSSTableFile("Keyspace1", "Standard1");
        ColumnFamily cfamily = ColumnFamily.create("Keyspace1", "Standard1");
        IPartitioner<?> partitioner = DatabaseDescriptor.getPartitioner();
        DataOutputBuffer dob = new DataOutputBuffer();
        SSTableWriter writer = new SSTableWriter(tempSS.getPath(), 2, partitioner);
        
        // Add rowA
        cfamily.addColumn(new QueryPath("Standard1", null, "name".getBytes()), "val".getBytes(), 1, false);
        ColumnFamily.serializer().serializeWithIndexes(cfamily, dob);
        writer.append(Util.dk("rowA"), dob);
        dob.reset();
        cfamily.clear();

        // Add rowExclude
        cfamily.addColumn(new QueryPath("Standard1", null, "name".getBytes()), "val".getBytes(), 1, false);
        ColumnFamily.serializer().serializeWithIndexes(cfamily, dob);
        writer.append(Util.dk("rowExclude"), dob);
        dob.reset();
        cfamily.clear();

        SSTableReader reader = writer.closeAndOpenReader();
        
        // Export to JSON and verify
        File tempJson = File.createTempFile("Standard1", ".json");
        SSTableExport.export(reader, new PrintStream(tempJson.getPath()), new String[]{"rowExclude"});
        
        // Import JSON to another SSTable file
        File tempSS2 = tempSSTableFile("Keyspace1", "Standard1");
        SSTableImport.importJson(tempJson.getPath(), "Keyspace1", "Standard1", tempSS2.getPath());        
        
        reader = SSTableReader.open(tempSS2.getPath(), DatabaseDescriptor.getPartitioner());
        QueryFilter qf = QueryFilter.getNamesFilter(Util.dk("rowA"), new QueryPath("Standard1", null, null), "name".getBytes());
        ColumnFamily cf = qf.getSSTableColumnIterator(reader).getColumnFamily();
        assertTrue(cf != null);
        assertTrue(Arrays.equals(cf.getColumn("name".getBytes()).value(), hexToBytes("76616c")));

        qf = QueryFilter.getNamesFilter(Util.dk("rowExclude"), new QueryPath("Standard1", null, null), "name".getBytes());
        cf = qf.getSSTableColumnIterator(reader).getColumnFamily();
        assert cf == null;
    }
}
