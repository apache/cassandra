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
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.filter.NamesQueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.SSTableReader;
import org.apache.cassandra.io.SSTableWriter;
import org.apache.cassandra.io.util.DataOutputBuffer;
import static org.apache.cassandra.Util.createTemporarySSTable;
import static org.apache.cassandra.utils.FBUtilities.hexToBytes;
import static org.junit.Assert.assertTrue;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.ParseException;
import org.junit.Test;

public class SSTableExportTest
{
    @Test
    public void testEnumeratekeys() throws IOException
    {
        File tempSS = createTemporarySSTable("Keyspace1", "Standard1");
        ColumnFamily cfamily = ColumnFamily.create("Keyspace1", "Standard1");
        IPartitioner<?> partitioner = DatabaseDescriptor.getPartitioner();
        DataOutputBuffer dob = new DataOutputBuffer();
        SSTableWriter writer = new SSTableWriter(tempSS.getPath(), 2, partitioner);
        
        // Add rowA
        cfamily.addColumn(new QueryPath("Standard1", null, "colA".getBytes()), "valA".getBytes(), 1, false);
        ColumnFamily.serializer().serializeWithIndexes(cfamily, dob);
        writer.append(partitioner.decorateKey("rowA"), dob);
        dob.reset();
        cfamily.clear();
        
        // Add rowB
        cfamily.addColumn(new QueryPath("Standard1", null, "colB".getBytes()), "valB".getBytes(), 1, false);
        ColumnFamily.serializer().serializeWithIndexes(cfamily, dob);
        writer.append(partitioner.decorateKey("rowB"), dob);
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
        assert output.equals("rowA" + sep + "rowB" + sep) : output;
    }

    @Test
    public void testExportSimpleCf() throws IOException    {
        File tempSS = createTemporarySSTable("Keyspace1", "Standard1");
        ColumnFamily cfamily = ColumnFamily.create("Keyspace1", "Standard1");
        IPartitioner<?> partitioner = DatabaseDescriptor.getPartitioner();
        DataOutputBuffer dob = new DataOutputBuffer();
        SSTableWriter writer = new SSTableWriter(tempSS.getPath(), 2, partitioner);
        
        // Add rowA
        cfamily.addColumn(new QueryPath("Standard1", null, "colA".getBytes()), "valA".getBytes(), 1, false);
        ColumnFamily.serializer().serializeWithIndexes(cfamily, dob);
        writer.append(partitioner.decorateKey("rowA"), dob);
        dob.reset();
        cfamily.clear();
        
        // Add rowB
        cfamily.addColumn(new QueryPath("Standard1", null, "colB".getBytes()), "valB".getBytes(), 1, false);
        ColumnFamily.serializer().serializeWithIndexes(cfamily, dob);
        writer.append(partitioner.decorateKey("rowB"), dob);
        dob.reset();
        cfamily.clear();
     
        SSTableReader reader = writer.closeAndOpenReader();
        
        // Export to JSON and verify
        File tempJson = File.createTempFile("Standard1", ".json");
        SSTableExport.export(reader, new PrintStream(tempJson.getPath()));
        
        JSONObject json = (JSONObject)JSONValue.parse(new FileReader(tempJson));
        
        JSONArray rowA = (JSONArray)json.get("rowA");
        JSONArray colA = (JSONArray)rowA.get(0);
        assert Arrays.equals(hexToBytes((String)colA.get(1)), "valA".getBytes());
        
        JSONArray rowB = (JSONArray)json.get("rowB");
        JSONArray colB = (JSONArray)rowB.get(0);
        assert !(Boolean)colB.get(3);
    }

    @Test
    public void testExportSuperCf() throws IOException
    {
        File tempSS = createTemporarySSTable("Keyspace1", "Super4");
        ColumnFamily cfamily = ColumnFamily.create("Keyspace1", "Super4");
        IPartitioner<?> partitioner = DatabaseDescriptor.getPartitioner();
        DataOutputBuffer dob = new DataOutputBuffer();
        SSTableWriter writer = new SSTableWriter(tempSS.getPath(), 2, partitioner);
        
        // Add rowA
        cfamily.addColumn(new QueryPath("Super4", "superA".getBytes(), "colA".getBytes()), "valA".getBytes(), 1, false);
        ColumnFamily.serializer().serializeWithIndexes(cfamily, dob);
        writer.append(partitioner.decorateKey("rowA"), dob);
        dob.reset();
        cfamily.clear();
        
        // Add rowB
        cfamily.addColumn(new QueryPath("Super4", "superB".getBytes(), "colB".getBytes()), "valB".getBytes(), 1, false);
        ColumnFamily.serializer().serializeWithIndexes(cfamily, dob);
        writer.append(partitioner.decorateKey("rowB"), dob);
        dob.reset();
        cfamily.clear();
     
        SSTableReader reader = writer.closeAndOpenReader();
        
        // Export to JSON and verify
        File tempJson = File.createTempFile("Super4", ".json");
        SSTableExport.export(reader, new PrintStream(tempJson.getPath()));
        
        JSONObject json = (JSONObject)JSONValue.parse(new FileReader(tempJson));
        
        JSONObject rowA = (JSONObject)json.get("rowA");
        JSONObject superA = (JSONObject)rowA.get(cfamily.getComparator().getString("superA".getBytes()));
        JSONArray subColumns = (JSONArray)superA.get("subColumns");
        JSONArray colA = (JSONArray)subColumns.get(0);
        
        assert Arrays.equals(hexToBytes((String)colA.get(1)), "valA".getBytes());
        assert !(Boolean)colA.get(3);       
    }
    
    @Test
    public void testRoundTripStandardCf() throws IOException, ParseException
    {
        File tempSS = createTemporarySSTable("Keyspace1", "Standard1");
        ColumnFamily cfamily = ColumnFamily.create("Keyspace1", "Standard1");
        IPartitioner<?> partitioner = DatabaseDescriptor.getPartitioner();
        DataOutputBuffer dob = new DataOutputBuffer();
        SSTableWriter writer = new SSTableWriter(tempSS.getPath(), 2, partitioner);
        
        // Add rowA
        cfamily.addColumn(new QueryPath("Standard1", null, "name".getBytes()), "val".getBytes(), 1, false);
        ColumnFamily.serializer().serializeWithIndexes(cfamily, dob);
        writer.append(partitioner.decorateKey("rowA"), dob);
        dob.reset();
        cfamily.clear();
        
        SSTableReader reader = writer.closeAndOpenReader();
        
        // Export to JSON and verify
        File tempJson = File.createTempFile("Standard1", ".json");
        SSTableExport.export(reader, new PrintStream(tempJson.getPath()));
        
        // Import JSON to another SSTable file
        File tempSS2 = createTemporarySSTable("Keyspace1", "Standard1");
        SSTableImport.importJson(tempJson.getPath(), "Keyspace1", "Standard1", tempSS2.getPath());        
        
        reader = SSTableReader.open(tempSS2.getPath(), DatabaseDescriptor.getPartitioner());
        NamesQueryFilter qf = new NamesQueryFilter("rowA", new QueryPath("Standard1", null, null), "name".getBytes());
        ColumnFamily cf = qf.getSSTableColumnIterator(reader).getColumnFamily();
        assertTrue(cf != null);
        assertTrue(Arrays.equals(cf.getColumn("name".getBytes()).value(), hexToBytes("76616c")));
    }
    
}
