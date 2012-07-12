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

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static org.apache.cassandra.io.sstable.SSTableUtils.tempSSTableFile;
import static org.apache.cassandra.utils.ByteBufferUtil.bytesToHex;
import static org.apache.cassandra.utils.ByteBufferUtil.hexToBytes;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Collections;

import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.CounterColumn;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.ExpiringColumn;
import org.apache.cassandra.db.SuperColumn;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableWriter;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.ParseException;

public class SSTableExportTest extends SchemaLoader
{
    public String asHex(String str)
    {
        return bytesToHex(ByteBufferUtil.bytes(str));
    }

    @Test
    public void testEnumeratekeys() throws IOException
    {
        File tempSS = tempSSTableFile("Keyspace1", "Standard1");
        ColumnFamily cfamily = ColumnFamily.create("Keyspace1", "Standard1");
        SSTableWriter writer = new SSTableWriter(tempSS.getPath(), 2);

        // Add rowA
        cfamily.addColumn(new QueryPath("Standard1", null, ByteBufferUtil.bytes("colA")), ByteBufferUtil.bytes("valA"), System.currentTimeMillis());
        writer.append(Util.dk("rowA"), cfamily);
        cfamily.clear();

        // Add rowB
        cfamily.addColumn(new QueryPath("Standard1", null, ByteBufferUtil.bytes("colB")), ByteBufferUtil.bytes("valB"), System.currentTimeMillis());
        writer.append(Util.dk("rowB"), cfamily);
        cfamily.clear();

        writer.closeAndOpenReader();

        // Enumerate and verify
        File temp = File.createTempFile("Standard1", ".txt");
        SSTableExport.enumeratekeys(Descriptor.fromFilename(writer.getFilename()), new PrintStream(temp.getPath()));


        FileReader file = new FileReader(temp);
        char[] buf = new char[(int) temp.length()];
        file.read(buf);
        String output = new String(buf);

        String sep = System.getProperty("line.separator");
        assert output.equals(asHex("rowA") + sep + asHex("rowB") + sep) : output;
    }

    @Test
    public void testExportSimpleCf() throws IOException, ParseException
    {
        File tempSS = tempSSTableFile("Keyspace1", "Standard1");
        ColumnFamily cfamily = ColumnFamily.create("Keyspace1", "Standard1");
        SSTableWriter writer = new SSTableWriter(tempSS.getPath(), 2);

        int nowInSec = (int)(System.currentTimeMillis() / 1000) + 42; //live for 42 seconds
        // Add rowA
        cfamily.addColumn(new QueryPath("Standard1", null, ByteBufferUtil.bytes("colA")), ByteBufferUtil.bytes("valA"), System.currentTimeMillis());
        cfamily.addColumn(null, new ExpiringColumn(ByteBufferUtil.bytes("colExp"), ByteBufferUtil.bytes("valExp"), System.currentTimeMillis(), 42, nowInSec));
        writer.append(Util.dk("rowA"), cfamily);
        cfamily.clear();

        // Add rowB
        cfamily.addColumn(new QueryPath("Standard1", null, ByteBufferUtil.bytes("colB")), ByteBufferUtil.bytes("valB"), System.currentTimeMillis());
        writer.append(Util.dk("rowB"), cfamily);
        cfamily.clear();

        // Add rowExclude
        cfamily.addColumn(new QueryPath("Standard1", null, ByteBufferUtil.bytes("colX")), ByteBufferUtil.bytes("valX"), System.currentTimeMillis());
        writer.append(Util.dk("rowExclude"), cfamily);
        cfamily.clear();

        SSTableReader reader = writer.closeAndOpenReader();

        // Export to JSON and verify
        File tempJson = File.createTempFile("Standard1", ".json");
        SSTableExport.export(reader, new PrintStream(tempJson.getPath()), new String[]{asHex("rowExclude")});

        JSONArray json = (JSONArray)JSONValue.parseWithException(new FileReader(tempJson));
        assertEquals("unexpected number of rows", 2, json.size());

        JSONObject rowA = (JSONObject)json.get(0);
        assertEquals("unexpected number of keys", 2, rowA.keySet().size());
        assertEquals("unexpected row key",asHex("rowA"),rowA.get("key"));

        JSONArray colsA = (JSONArray)rowA.get("columns");
        JSONArray colA = (JSONArray)colsA.get(0);
        assert hexToBytes((String)colA.get(1)).equals(ByteBufferUtil.bytes("valA"));

        JSONArray colExp = (JSONArray)colsA.get(1);
        assert ((Long)colExp.get(4)) == 42;
        assert ((Long)colExp.get(5)) == nowInSec;

        JSONObject rowB = (JSONObject)json.get(1);
        assertEquals("unexpected number of keys", 2, rowB.keySet().size());
        assertEquals("unexpected row key",asHex("rowB"),rowB.get("key"));

        JSONArray colsB = (JSONArray)rowB.get("columns");
        JSONArray colB = (JSONArray)colsB.get(0);
        assert colB.size() == 3;

    }

    @Test
    public void testExportSuperCf() throws IOException, ParseException
    {
        File tempSS = tempSSTableFile("Keyspace1", "Super4");
        ColumnFamily cfamily = ColumnFamily.create("Keyspace1", "Super4");
        SSTableWriter writer = new SSTableWriter(tempSS.getPath(), 2);

        // Add rowA
        cfamily.addColumn(new QueryPath("Super4", ByteBufferUtil.bytes("superA"), ByteBufferUtil.bytes("colA")), ByteBufferUtil.bytes("valA"), System.currentTimeMillis());
        // set deletion info on the super col
        ((SuperColumn) cfamily.getColumn(ByteBufferUtil.bytes("superA"))).setDeletionInfo(new DeletionInfo(0, 0));
        writer.append(Util.dk("rowA"), cfamily);
        cfamily.clear();

        // Add rowB
        cfamily.addColumn(new QueryPath("Super4", ByteBufferUtil.bytes("superB"), ByteBufferUtil.bytes("colB")), ByteBufferUtil.bytes("valB"), System.currentTimeMillis());
        writer.append(Util.dk("rowB"), cfamily);
        cfamily.clear();

        // Add rowExclude
        cfamily.addColumn(new QueryPath("Super4", ByteBufferUtil.bytes("superX"), ByteBufferUtil.bytes("colX")), ByteBufferUtil.bytes("valX"), System.currentTimeMillis());
        writer.append(Util.dk("rowExclude"), cfamily);
        cfamily.clear();

        SSTableReader reader = writer.closeAndOpenReader();

        // Export to JSON and verify
        File tempJson = File.createTempFile("Super4", ".json");
        SSTableExport.export(reader, new PrintStream(tempJson.getPath()), new String[] { asHex("rowExclude") });

        JSONArray json = (JSONArray) JSONValue.parseWithException(new FileReader(tempJson));
        assertEquals("unexpected number of rows", 2, json.size());

        // make sure only the two rows we expect are there
        JSONObject rowA = (JSONObject) json.get(0);
        assertEquals("unexpected number of keys", 2, rowA.keySet().size());
        assertEquals("unexpected row key", asHex("rowA"), rowA.get("key"));
        JSONObject rowB = (JSONObject) json.get(0);
        assertEquals("unexpected number of keys", 2, rowB.keySet().size());
        assertEquals("unexpected row key", asHex("rowA"), rowB.get("key"));

        JSONObject cols = (JSONObject) rowA.get("columns");

        JSONObject superA = (JSONObject) cols.get(cfamily.getComparator().getString(ByteBufferUtil.bytes("superA")));
        JSONArray subColumns = (JSONArray) superA.get("subColumns");
        JSONArray colA = (JSONArray) subColumns.get(0);
        assert hexToBytes((String) colA.get(1)).equals(ByteBufferUtil.bytes("valA"));
        assert colA.size() == 3;
    }

    @Test
    public void testRoundTripStandardCf() throws IOException, ParseException
    {
        File tempSS = tempSSTableFile("Keyspace1", "Standard1");
        ColumnFamily cfamily = ColumnFamily.create("Keyspace1", "Standard1");
        SSTableWriter writer = new SSTableWriter(tempSS.getPath(), 2);

        // Add rowA
        cfamily.addColumn(new QueryPath("Standard1", null, ByteBufferUtil.bytes("name")), ByteBufferUtil.bytes("val"), System.currentTimeMillis());
        writer.append(Util.dk("rowA"), cfamily);
        cfamily.clear();

        // Add rowExclude
        cfamily.addColumn(new QueryPath("Standard1", null, ByteBufferUtil.bytes("name")), ByteBufferUtil.bytes("val"), System.currentTimeMillis());
        writer.append(Util.dk("rowExclude"), cfamily);
        cfamily.clear();

        SSTableReader reader = writer.closeAndOpenReader();

        // Export to JSON and verify
        File tempJson = File.createTempFile("Standard1", ".json");
        SSTableExport.export(reader, new PrintStream(tempJson.getPath()), new String[]{asHex("rowExclude")});

        // Import JSON to another SSTable file
        File tempSS2 = tempSSTableFile("Keyspace1", "Standard1");
        new SSTableImport().importJson(tempJson.getPath(), "Keyspace1", "Standard1", tempSS2.getPath());

        reader = SSTableReader.open(Descriptor.fromFilename(tempSS2.getPath()));
        QueryFilter qf = QueryFilter.getNamesFilter(Util.dk("rowA"), new QueryPath("Standard1", null, null), ByteBufferUtil.bytes("name"));
        ColumnFamily cf = qf.getSSTableColumnIterator(reader).getColumnFamily();
        qf.collateOnDiskAtom(cf, Collections.singletonList(qf.getSSTableColumnIterator(reader)), Integer.MIN_VALUE);
        assertTrue(cf != null);
        assertTrue(cf.getColumn(ByteBufferUtil.bytes("name")).value().equals(hexToBytes("76616c")));

        qf = QueryFilter.getNamesFilter(Util.dk("rowExclude"), new QueryPath("Standard1", null, null), ByteBufferUtil.bytes("name"));
        cf = qf.getSSTableColumnIterator(reader).getColumnFamily();
        assert cf == null;
    }

    @Test
    public void testExportCounterCf() throws IOException, ParseException
    {
        File tempSS = tempSSTableFile("Keyspace1", "Counter1");
        ColumnFamily cfamily = ColumnFamily.create("Keyspace1", "Counter1");
        SSTableWriter writer = new SSTableWriter(tempSS.getPath(), 2);

        // Add rowA
        cfamily.addColumn(null, new CounterColumn(ByteBufferUtil.bytes("colA"), 42, System.currentTimeMillis()));
        writer.append(Util.dk("rowA"), cfamily);
        cfamily.clear();

        SSTableReader reader = writer.closeAndOpenReader();

        // Export to JSON and verify
        File tempJson = File.createTempFile("Counter1", ".json");
        SSTableExport.export(reader, new PrintStream(tempJson.getPath()), new String[0]);
        JSONArray json = (JSONArray)JSONValue.parseWithException(new FileReader(tempJson));
        assertEquals("unexpected number of rows", 1, json.size());

        JSONObject row = (JSONObject)json.get(0);
        assertEquals("unexpected number of keys", 2, row.keySet().size());
        assertEquals("unexpected row key",asHex("rowA"),row.get("key"));

        JSONArray cols = (JSONArray)row.get("columns");
        JSONArray colA = (JSONArray)cols.get(0);
        assert hexToBytes((String)colA.get(0)).equals(ByteBufferUtil.bytes("colA"));
        assert ((String) colA.get(3)).equals("c");
        assert (Long) colA.get(4) == Long.MIN_VALUE;
    }

    @Test
    public void testEscapingDoubleQuotes() throws IOException, ParseException
    {
        File tempSS = tempSSTableFile("Keyspace1", "ValuesWithQuotes");
        ColumnFamily cfamily = ColumnFamily.create("Keyspace1", "ValuesWithQuotes");
        SSTableWriter writer = new SSTableWriter(tempSS.getPath(), 2);

        // Add rowA
        cfamily.addColumn(null, new Column(ByteBufferUtil.bytes("data"), UTF8Type.instance.fromString("{\"foo\":\"bar\"}")));
        writer.append(Util.dk("rowA"), cfamily);
        cfamily.clear();

        SSTableReader reader = writer.closeAndOpenReader();

        // Export to JSON and verify
        File tempJson = File.createTempFile("ValuesWithQuotes", ".json");
        SSTableExport.export(reader, new PrintStream(tempJson.getPath()), new String[0]);

        JSONArray json = (JSONArray)JSONValue.parseWithException(new FileReader(tempJson));
        assertEquals("unexpected number of rows", 1, json.size());

        JSONObject row = (JSONObject)json.get(0);
        assertEquals("unexpected number of keys", 2, row.keySet().size());
        assertEquals("unexpected row key",asHex("rowA"),row.get("key"));

        JSONArray cols = (JSONArray)row.get("columns");
        JSONArray colA = (JSONArray)cols.get(0);
        assert hexToBytes((String)colA.get(0)).equals(ByteBufferUtil.bytes("data"));
        assert colA.get(1).equals("{\"foo\":\"bar\"}");
    }

    @Test
    public void testExportColumnsWithMetadata() throws IOException, ParseException
    {

        File tempSS = tempSSTableFile("Keyspace1", "Standard1");
        ColumnFamily cfamily = ColumnFamily.create("Keyspace1", "Standard1");
        SSTableWriter writer = new SSTableWriter(tempSS.getPath(), 2);

        // Add rowA
        cfamily.addColumn(new QueryPath("Standard1", null, ByteBufferUtil.bytes("colName")),
                ByteBufferUtil.bytes("val"), System.currentTimeMillis());
        cfamily.addColumn(new QueryPath("Standard1", null, ByteBufferUtil.bytes("colName1")),
                ByteBufferUtil.bytes("val1"), System.currentTimeMillis());
        cfamily.delete(new DeletionInfo(0, 0));
        writer.append(Util.dk("rowA"), cfamily);

        SSTableReader reader = writer.closeAndOpenReader();
        // Export to JSON and verify
        File tempJson = File.createTempFile("CFWithDeletionInfo", ".json");
        SSTableExport.export(reader, new PrintStream(tempJson.getPath()), new String[0]);

        JSONArray json = (JSONArray)JSONValue.parseWithException(new FileReader(tempJson));
        System.out.println(json.toJSONString());
        assertEquals("unexpected number of rows", 1, json.size());

        JSONObject row = (JSONObject)json.get(0);
        assertEquals("unexpected number of keys", 3, row.keySet().size());
        assertEquals("unexpected row key",asHex("rowA"),row.get("key"));

        // check that the row key is there and present
        String rowKey = (String) row.get("key");
        assertNotNull("expecing key to be present", rowKey);
        assertEquals("key did not match", ByteBufferUtil.bytes("rowA"), hexToBytes(rowKey));

        // check that there is metadata and that it contains deletionInfo
        JSONObject meta = (JSONObject) row.get("metadata");
        assertNotNull("expecing metadata to be present", meta);

        assertEquals("unexpected number of metadata entries", 1, meta.keySet().size());

        JSONObject serializedDeletionInfo = (JSONObject) meta.get("deletionInfo");
        assertNotNull("expecing deletionInfo to be present", serializedDeletionInfo);

        assertEquals(
                "unexpected serialization format for topLevelDeletion",
                "{\"markedForDeleteAt\":0,\"localDeletionTime\":0}",
                serializedDeletionInfo.toJSONString());

        // check the colums are what we put in
        JSONArray cols = (JSONArray) row.get("columns");
        assertNotNull("expecing columns to be present", cols);
        assertEquals("expecting two columns", 2, cols.size());

        JSONArray col1 = (JSONArray) cols.get(0);
        assertEquals("column name did not match", ByteBufferUtil.bytes("colName"), hexToBytes((String) col1.get(0)));
        assertEquals("column value did not match", ByteBufferUtil.bytes("val"), hexToBytes((String) col1.get(1)));

        JSONArray col2 = (JSONArray) cols.get(1);
        assertEquals("column name did not match", ByteBufferUtil.bytes("colName1"), hexToBytes((String) col2.get(0)));
        assertEquals("column value did not match", ByteBufferUtil.bytes("val1"), hexToBytes((String) col2.get(1)));

    }
}
