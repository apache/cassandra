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

package org.apache.cassandra.pig;

import java.io.IOException;
import java.util.Iterator;

import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.Hex;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.thrift.TException;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class CqlTableDataTypeTest extends PigTestBase
{
    //ASCII    (AsciiType.instance),
    //BIGINT   (LongType.instance),
    //BLOB     (BytesType.instance),
    //BOOLEAN  (BooleanType.instance),
    //COUNTER  (CounterColumnType.instance),
    //DECIMAL  (DecimalType.instance),
    //DOUBLE   (DoubleType.instance),
    //FLOAT    (FloatType.instance),
    //INET     (InetAddressType.instance),
    //INT      (Int32Type.instance),
    //TEXT     (UTF8Type.instance),
    //TIMESTAMP(DateType.instance),
    //UUID     (UUIDType.instance),
    //VARCHAR  (UTF8Type.instance),
    //VARINT   (IntegerType.instance),
    //TIMEUUID (TimeUUIDType.instance);
    //SET
    //LIST
    //MAP
    //Create table to test the above data types
    private static String[] statements = {
            "DROP KEYSPACE IF EXISTS cql3ks",
            "CREATE KEYSPACE cql3ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}",
            "USE cql3ks;",

            "CREATE TABLE cqltable (" +
            "key int primary key," +
            "col_ascii ascii," +
            "col_bigint bigint," +
            "col_blob blob," +
            "col_boolean boolean," +
            "col_decimal decimal," +
            "col_double double," +
            "col_float float," +
            "col_inet inet," +
            "col_int int," +
            "col_text text," +
            "col_timestamp timestamp," +
            "col_uuid uuid," +
            "col_varchar varchar," +
            "col_varint varint," +
            "col_timeuuid timeuuid);",

            "CREATE TABLE settable (" +
            "key int primary key," +
            "col_set_ascii set<ascii>," +
            "col_set_bigint set<bigint>," +
            "col_set_blob set<blob>," +
            "col_set_boolean set<boolean>," +
            "col_set_decimal set<decimal>," +
            "col_set_double set<double>," +
            "col_set_float set<float>," +
            "col_set_inet set<inet>," +
            "col_set_int set<int>," +
            "col_set_text set<text>," +
            "col_set_timestamp set<timestamp>," +
            "col_set_uuid set<uuid>," +
            "col_set_varchar set<varchar>," +
            "col_set_varint set<varint>," +
            "col_set_timeuuid set<timeuuid>);",

            "CREATE TABLE listtable (" +
            "key int primary key," +
            "col_list_ascii list<ascii>," +
            "col_list_bigint list<bigint>," +
            "col_list_blob list<blob>," +
            "col_list_boolean list<boolean>," +
            "col_list_decimal list<decimal>," +
            "col_list_double list<double>," +
            "col_list_float list<float>," +
            "col_list_inet list<inet>," +
            "col_list_int list<int>," +
            "col_list_text list<text>," +
            "col_list_timestamp list<timestamp>," +
            "col_list_uuid list<uuid>," +
            "col_list_varchar list<varchar>," +
            "col_list_varint list<varint>," +
            "col_list_timeuuid list<timeuuid>);",

            "CREATE TABLE maptable (" +
            "key int primary key," +
            "col_map_ascii map<ascii, ascii>," +
            "col_map_bigint map<bigint, bigint>," +
            "col_map_blob map<blob, blob>," +
            "col_map_boolean map<boolean, boolean>," +
            "col_map_decimal map<decimal, decimal>," +
            "col_map_double map<double, double>," +
            "col_map_float map<float, float>," +
            "col_map_inet map<inet, inet>," +
            "col_map_int map<int, int>," +
            "col_map_text map<text, text>," +
            "col_map_timestamp map<timestamp, timestamp>," +
            "col_map_uuid map<uuid, uuid>," +
            "col_map_varchar map<varchar, varchar>," +
            "col_map_varint map<varint, varint>," +
            "col_map_timeuuid map<timeuuid, timeuuid>);",
        
            "INSERT INTO cqltable(key, col_ascii) VALUES (1, 'ascii');",
            "INSERT INTO cqltable(key, col_bigint) VALUES (1, 12345678);",
            "INSERT INTO cqltable(key, col_blob) VALUES (1, 0x23446c6c6f);",
            "INSERT INTO cqltable(key, col_boolean) VALUES (1, false);",
            "INSERT INTO cqltable(key, col_decimal) VALUES (1, 23.4567);",
            "INSERT INTO cqltable(key, col_double) VALUES (1, 12345678.12345678);",
            "INSERT INTO cqltable(key, col_float) VALUES (1, 123.12);",
            "INSERT INTO cqltable(key, col_inet) VALUES (1, '127.0.0.1');",
            "INSERT INTO cqltable(key, col_int) VALUES (1, 123);",
            "INSERT INTO cqltable(key, col_text) VALUES (1, 'text');",
            "INSERT INTO cqltable(key, col_timestamp) VALUES (1, '2011-02-03T04:05:00+0000');",
            "INSERT INTO cqltable(key, col_timeuuid) VALUES (1, maxTimeuuid('2013-01-01 00:05+0000'));",
            "INSERT INTO cqltable(key, col_uuid) VALUES (1, 550e8400-e29b-41d4-a716-446655440000);",
            "INSERT INTO cqltable(key, col_varchar) VALUES (1, 'varchar');",
            "INSERT INTO cqltable(key, col_varint) VALUES (1, 123);",

            "INSERT INTO settable(key, col_set_ascii) VALUES (1, {'ascii1', 'ascii2'});",
            "INSERT INTO settable(key, col_set_bigint) VALUES (1, {12345678, 12345679});",
            "INSERT INTO settable(key, col_set_blob) VALUES (1, {0x68656c6c6f, 0x68656c6c6e});",
            "INSERT INTO settable(key, col_set_boolean) VALUES (1, {false, true});",
            "INSERT INTO settable(key, col_set_decimal) VALUES (1, {23.4567, 23.4568});",
            "INSERT INTO settable(key, col_set_double) VALUES (1, {12345678.12345678, 12345678.12345679});",
            "INSERT INTO settable(key, col_set_float) VALUES (1, {123.12, 123.13});",
            "INSERT INTO settable(key, col_set_inet) VALUES (1, {'127.0.0.1', '127.0.0.2'});",
            "INSERT INTO settable(key, col_set_int) VALUES (1, {123, 124});",
            "INSERT INTO settable(key, col_set_text) VALUES (1, {'text1', 'text2'});",
            "INSERT INTO settable(key, col_set_timestamp) VALUES (1, {'2011-02-03T04:05:00+0000', '2011-02-04T04:05:00+0000'});",
            "INSERT INTO settable(key, col_set_timeuuid) VALUES (1, {e23f450f-53a6-11e2-7f7f-7f7f7f7f7f7f, e23f450f-53a6-11e2-7f7f-7f7f7f7f7f77});",      
            "INSERT INTO settable(key, col_set_uuid) VALUES (1, {550e8400-e29b-41d4-a716-446655440000, 550e8400-e29b-41d4-a716-446655440001});",
            "INSERT INTO settable(key, col_set_varchar) VALUES (1, {'varchar1', 'varchar2'});",
            "INSERT INTO settable(key, col_set_varint) VALUES (1, {123, 124});",

            "INSERT INTO listtable(key, col_list_ascii) VALUES (1, ['ascii2', 'ascii1']);",
            "INSERT INTO listtable(key, col_list_bigint) VALUES (1, [12345679, 12345678]);",
            "INSERT INTO listtable(key, col_list_blob) VALUES (1, [0x68656c6c6e, 0x68656c6c6f]);",
            "INSERT INTO listtable(key, col_list_boolean) VALUES (1, [true, false]);",
            "INSERT INTO listtable(key, col_list_decimal) VALUES (1, [23.4568, 23.4567]);",
            "INSERT INTO listtable(key, col_list_double) VALUES (1, [12345678.12345679, 12345678.12345678]);",
            "INSERT INTO listtable(key, col_list_float) VALUES (1, [123.13, 123.12]);",
            "INSERT INTO listtable(key, col_list_inet) VALUES (1, ['127.0.0.2', '127.0.0.1']);",
            "INSERT INTO listtable(key, col_list_int) VALUES (1, [124, 123]);",
            "INSERT INTO listtable(key, col_list_text) VALUES (1, ['text2', 'text1']);",
            "INSERT INTO listtable(key, col_list_timestamp) VALUES (1, ['2011-02-04T04:05:00+0000', '2011-02-03T04:05:00+0000']);",
            "INSERT INTO listtable(key, col_list_timeuuid) VALUES (1, [e23f450f-53a6-11e2-7f7f-7f7f7f7f7f77, e23f450f-53a6-11e2-7f7f-7f7f7f7f7f7f]);",
            "INSERT INTO listtable(key, col_list_uuid) VALUES (1, [550e8400-e29b-41d4-a716-446655440001, 550e8400-e29b-41d4-a716-446655440000]);",
            "INSERT INTO listtable(key, col_list_varchar) VALUES (1, ['varchar2', 'varchar1']);",
            "INSERT INTO listtable(key, col_list_varint) VALUES (1, [124, 123]);",

            "INSERT INTO maptable(key, col_map_ascii) VALUES (1, {'ascii1' : 'ascii2'});",
            "INSERT INTO maptable(key, col_map_bigint) VALUES (1, {12345678 : 12345679});",
            "INSERT INTO maptable(key, col_map_blob) VALUES (1, {0x68656c6c6f : 0x68656c6c6e});",
            "INSERT INTO maptable(key, col_map_boolean) VALUES (1, {false : true});",
            "INSERT INTO maptable(key, col_map_decimal) VALUES (1, {23.4567 : 23.4568});",
            "INSERT INTO maptable(key, col_map_double) VALUES (1, {12345678.12345678 : 12345678.12345679});",
            "INSERT INTO maptable(key, col_map_float) VALUES (1, {123.12 : 123.13});",
            "INSERT INTO maptable(key, col_map_inet) VALUES (1, {'127.0.0.1' : '127.0.0.2'});",
            "INSERT INTO maptable(key, col_map_int) VALUES (1, {123 : 124});",
            "INSERT INTO maptable(key, col_map_text) VALUES (1, {'text1' : 'text2'});",
            "INSERT INTO maptable(key, col_map_timestamp) VALUES (1, {'2011-02-03T04:05:00+0000' : '2011-02-04T04:05:00+0000'});",
            "INSERT INTO maptable(key, col_map_timeuuid) VALUES (1, {e23f450f-53a6-11e2-7f7f-7f7f7f7f7f7f : e23f450f-53a6-11e2-7f7f-7f7f7f7f7f77});",      
            "INSERT INTO maptable(key, col_map_uuid) VALUES (1, {550e8400-e29b-41d4-a716-446655440000 : 550e8400-e29b-41d4-a716-446655440001});",
            "INSERT INTO maptable(key, col_map_varchar) VALUES (1, {'varchar1' : 'varchar2'});",
            "INSERT INTO maptable(key, col_map_varint) VALUES (1, {123 : 124});",

            "CREATE TABLE countertable (key int primary key, col_counter counter);",            
            "UPDATE countertable SET col_counter = col_counter + 3 WHERE key = 1;",
    };

    @BeforeClass
    public static void setup() throws IOException, ConfigurationException, TException
    {
        startCassandra();
        executeCQLStatements(statements);
        startHadoopCluster();
    }

    @Test
    public void testCqlNativeStorageRegularType() throws IOException
    {
        //input_cql=select * from cqltable where token(key) > ? and token(key) <= ?
        cqlTableTest("rows = LOAD 'cql://cql3ks/cqltable?" + defaultParameters + nativeParameters + "&input_cql=select%20*%20from%20cqltable%20where%20token(key)%20%3E%20%3F%20and%20token(key)%20%3C%3D%20%3F' USING CqlNativeStorage();");

        //input_cql=select * from countertable where token(key) > ? and token(key) <= ?
        counterTableTest("cc_rows = LOAD 'cql://cql3ks/countertable?" + defaultParameters + nativeParameters + "&input_cql=select%20*%20from%20countertable%20where%20token(key)%20%3E%20%3F%20and%20token(key)%20%3C%3D%20%3F' USING CqlNativeStorage();");
    }

    private void cqlTableTest(String initialQuery) throws IOException
    {
        pig.registerQuery(initialQuery);
        Iterator<Tuple> it = pig.openIterator("rows");
        //{key: int, 
        //col_ascii: chararray, 
        //col_bigint: long, 
        //col_blob: bytearray, 
        //col_boolean: bytearray,
        //col_decimal: chararray, 
        //col_double: double, 
        //col_float: float, 
        //col_inet: chararray, 
        //col_int: int,
        //col_text: chararray, 
        //col_timestamp: long, 
        //col_timeuuid: bytearray, 
        //col_uuid: chararray,
        //col_varchar: chararray, 
        //col_varint: int}
        if (it.hasNext()) {
            Tuple t = it.next();
            Assert.assertEquals(t.get(0), 1);
            Assert.assertEquals(t.get(1), "ascii");
            Assert.assertEquals(t.get(2), 12345678L);
            Assert.assertEquals(t.get(3), new DataByteArray(Hex.hexToBytes("23446c6c6f")));
            Assert.assertEquals(t.get(4), false);
            Assert.assertEquals(t.get(5), "23.4567");
            Assert.assertEquals(t.get(6), 12345678.12345678d);
            Assert.assertEquals(t.get(7), 123.12f);
            Assert.assertEquals(t.get(8), "127.0.0.1");
            Assert.assertEquals(t.get(9), 123);
            Assert.assertEquals(t.get(10), "text");
            Assert.assertEquals(t.get(11), 1296705900000L);
            Assert.assertEquals(t.get(12), new DataByteArray((TimeUUIDType.instance.fromString("e23f450f-53a6-11e2-7f7f-7f7f7f7f7f7f").array())));
            Assert.assertEquals(t.get(13), new DataByteArray((UUIDType.instance.fromString("550e8400-e29b-41d4-a716-446655440000").array())));
            Assert.assertEquals(t.get(14), "varchar");
            Assert.assertEquals(t.get(15), 123);
        }
        else
        {
            Assert.fail("Failed to get data for query " + initialQuery);
        }
    }

    private void counterTableTest(String initialQuery) throws IOException
    {
        pig.registerQuery(initialQuery);
        Iterator<Tuple>  it = pig.openIterator("cc_rows");
        if (it.hasNext()) {
            Tuple t = it.next();
            Assert.assertEquals(t.get(0), 1);
            Assert.assertEquals(t.get(1), 3L);
        }
        else
        {
            Assert.fail("Failed to get data for query " + initialQuery);
        }
    }

    @Test
    public void testCqlNativeStorageSetType() throws IOException
    {
        //input_cql=select * from settable where token(key) > ? and token(key) <= ?
        settableTest("set_rows = LOAD 'cql://cql3ks/settable?" + defaultParameters + nativeParameters + "&input_cql=select%20*%20from%20settable%20where%20token(key)%20%3E%20%3F%20and%20token(key)%20%3C%3D%20%3F' USING CqlNativeStorage();");
    }

    private void settableTest(String initialQuery) throws IOException
    {
        pig.registerQuery(initialQuery);
        Iterator<Tuple> it = pig.openIterator("set_rows");
        if (it.hasNext()) {
            Tuple t = it.next();
            Assert.assertEquals(t.get(0), 1);
            Tuple innerTuple = (Tuple) t.get(1);
            Assert.assertEquals(innerTuple.get(0), "ascii1");
            Assert.assertEquals(innerTuple.get(1), "ascii2");
            innerTuple = (Tuple) t.get(2);
            Assert.assertEquals(innerTuple.get(0), 12345678L);
            Assert.assertEquals(innerTuple.get(1), 12345679L);
            innerTuple = (Tuple) t.get(3);
            Assert.assertEquals(innerTuple.get(0), new DataByteArray(Hex.hexToBytes("68656c6c6e")));
            Assert.assertEquals(innerTuple.get(1), new DataByteArray(Hex.hexToBytes("68656c6c6f")));
            innerTuple = (Tuple) t.get(4);
            Assert.assertEquals(innerTuple.get(0), false);
            Assert.assertEquals(innerTuple.get(1), true);
            innerTuple = (Tuple) t.get(5);
            Assert.assertEquals(innerTuple.get(0), "23.4567");
            Assert.assertEquals(innerTuple.get(1), "23.4568");
            innerTuple = (Tuple) t.get(6);
            Assert.assertEquals(innerTuple.get(0), 12345678.12345678d);
            Assert.assertEquals(innerTuple.get(1), 12345678.12345679d);
            innerTuple = (Tuple) t.get(7);
            Assert.assertEquals(innerTuple.get(0), 123.12f);
            Assert.assertEquals(innerTuple.get(1), 123.13f);
            innerTuple = (Tuple) t.get(8);
            Assert.assertEquals(innerTuple.get(0), "127.0.0.1");
            Assert.assertEquals(innerTuple.get(1), "127.0.0.2");
            innerTuple = (Tuple) t.get(9);
            Assert.assertEquals(innerTuple.get(0), 123);
            Assert.assertEquals(innerTuple.get(1), 124);
            innerTuple = (Tuple) t.get(10);
            Assert.assertEquals(innerTuple.get(0), "text1");
            Assert.assertEquals(innerTuple.get(1), "text2");
            innerTuple = (Tuple) t.get(11);
            Assert.assertEquals(innerTuple.get(0), 1296705900000L);
            Assert.assertEquals(innerTuple.get(1), 1296792300000L);
            innerTuple = (Tuple) t.get(12);
            Assert.assertEquals(innerTuple.get(0), new DataByteArray((TimeUUIDType.instance.fromString("e23f450f-53a6-11e2-7f7f-7f7f7f7f7f77").array())));
            Assert.assertEquals(innerTuple.get(1), new DataByteArray((TimeUUIDType.instance.fromString("e23f450f-53a6-11e2-7f7f-7f7f7f7f7f7f").array())));
            innerTuple = (Tuple) t.get(13);
            Assert.assertEquals(innerTuple.get(0), new DataByteArray((UUIDType.instance.fromString("550e8400-e29b-41d4-a716-446655440000").array())));
            Assert.assertEquals(innerTuple.get(1), new DataByteArray((UUIDType.instance.fromString("550e8400-e29b-41d4-a716-446655440001").array())));
            innerTuple = (Tuple) t.get(14);
            Assert.assertEquals(innerTuple.get(0), "varchar1");
            Assert.assertEquals(innerTuple.get(1), "varchar2");  
            innerTuple = (Tuple) t.get(15);
            Assert.assertEquals(innerTuple.get(0), 123);
            Assert.assertEquals(innerTuple.get(1), 124);
        }
        else
        {
            Assert.fail("Failed to get data for query " + initialQuery);
        }
    }

    @Test
    public void testCqlNativeStorageListType() throws IOException
    {
        //input_cql=select * from listtable where token(key) > ? and token(key) <= ?
        listtableTest("list_rows = LOAD 'cql://cql3ks/listtable?" + defaultParameters + nativeParameters + "&input_cql=select%20*%20from%20listtable%20where%20token(key)%20%3E%20%3F%20and%20token(key)%20%3C%3D%20%3F' USING CqlNativeStorage();");
    }

    private void listtableTest(String initialQuery) throws IOException
    {
        pig.registerQuery(initialQuery);
        Iterator<Tuple> it = pig.openIterator("list_rows");
        if (it.hasNext()) {
            Tuple t = it.next();
            Assert.assertEquals(t.get(0), 1);
            Tuple innerTuple = (Tuple) t.get(1);
            Assert.assertEquals(innerTuple.get(1), "ascii1");
            Assert.assertEquals(innerTuple.get(0), "ascii2");
            innerTuple = (Tuple) t.get(2);
            Assert.assertEquals(innerTuple.get(1), 12345678L);
            Assert.assertEquals(innerTuple.get(0), 12345679L);
            innerTuple = (Tuple) t.get(3);
            Assert.assertEquals(innerTuple.get(1), new DataByteArray(Hex.hexToBytes("68656c6c6f")));
            Assert.assertEquals(innerTuple.get(0), new DataByteArray(Hex.hexToBytes("68656c6c6e")));
            innerTuple = (Tuple) t.get(4);
            Assert.assertEquals(innerTuple.get(1), false);
            Assert.assertEquals(innerTuple.get(0), true);
            innerTuple = (Tuple) t.get(5);
            Assert.assertEquals(innerTuple.get(1), "23.4567");
            Assert.assertEquals(innerTuple.get(0), "23.4568");
            innerTuple = (Tuple) t.get(6);
            Assert.assertEquals(innerTuple.get(1), 12345678.12345678d);
            Assert.assertEquals(innerTuple.get(0), 12345678.12345679d);
            innerTuple = (Tuple) t.get(7);
            Assert.assertEquals(innerTuple.get(1), 123.12f);
            Assert.assertEquals(innerTuple.get(0), 123.13f);
            innerTuple = (Tuple) t.get(8);
            Assert.assertEquals(innerTuple.get(1), "127.0.0.1");
            Assert.assertEquals(innerTuple.get(0), "127.0.0.2");
            innerTuple = (Tuple) t.get(9);
            Assert.assertEquals(innerTuple.get(1), 123);
            Assert.assertEquals(innerTuple.get(0), 124);
            innerTuple = (Tuple) t.get(10);
            Assert.assertEquals(innerTuple.get(1), "text1");
            Assert.assertEquals(innerTuple.get(0), "text2");
            innerTuple = (Tuple) t.get(11);
            Assert.assertEquals(innerTuple.get(1), 1296705900000L);
            Assert.assertEquals(innerTuple.get(0), 1296792300000L);
            innerTuple = (Tuple) t.get(12);
            Assert.assertEquals(innerTuple.get(1), new DataByteArray((TimeUUIDType.instance.fromString("e23f450f-53a6-11e2-7f7f-7f7f7f7f7f7f").array())));
            Assert.assertEquals(innerTuple.get(0), new DataByteArray((TimeUUIDType.instance.fromString("e23f450f-53a6-11e2-7f7f-7f7f7f7f7f77").array())));
            innerTuple = (Tuple) t.get(13);
            Assert.assertEquals(innerTuple.get(1), new DataByteArray((UUIDType.instance.fromString("550e8400-e29b-41d4-a716-446655440000").array())));
            Assert.assertEquals(innerTuple.get(0), new DataByteArray((UUIDType.instance.fromString("550e8400-e29b-41d4-a716-446655440001").array())));
            innerTuple = (Tuple) t.get(14);
            Assert.assertEquals(innerTuple.get(1), "varchar1");
            Assert.assertEquals(innerTuple.get(0), "varchar2");  
            innerTuple = (Tuple) t.get(15);
            Assert.assertEquals(innerTuple.get(1), 123);
            Assert.assertEquals(innerTuple.get(0), 124);
        }
        else
        {
            Assert.fail("Failed to get data for query " + initialQuery);
        }
    }

    @Test
    public void testCqlNativeStorageMapType() throws IOException
    {
        //input_cql=select * from maptable where token(key) > ? and token(key) <= ?
        maptableTest("map_rows = LOAD 'cql://cql3ks/maptable?" + defaultParameters + nativeParameters + "&input_cql=select%20*%20from%20maptable%20where%20token(key)%20%3E%20%3F%20and%20token(key)%20%3C%3D%20%3F' USING CqlNativeStorage();");
    }

    private void maptableTest(String initialQuery) throws IOException
    {
        pig.registerQuery(initialQuery);
        Iterator<Tuple> it = pig.openIterator("map_rows");
        if (it.hasNext()) {
            Tuple t = it.next();
            Assert.assertEquals(t.get(0), 1);
            Tuple innerTuple = (Tuple) ((Tuple) t.get(1)).get(0);
            Assert.assertEquals(innerTuple.get(0), "ascii1");
            Assert.assertEquals(innerTuple.get(1), "ascii2");
            innerTuple = (Tuple) ((Tuple) t.get(2)).get(0);
            Assert.assertEquals(innerTuple.get(0), 12345678L);
            Assert.assertEquals(innerTuple.get(1), 12345679L);
            innerTuple = (Tuple) ((Tuple) t.get(3)).get(0);
            Assert.assertEquals(innerTuple.get(0), new DataByteArray(Hex.hexToBytes("68656c6c6f")));
            Assert.assertEquals(innerTuple.get(1), new DataByteArray(Hex.hexToBytes("68656c6c6e")));
            innerTuple = (Tuple) ((Tuple) t.get(4)).get(0);
            Assert.assertEquals(innerTuple.get(0), false);
            Assert.assertEquals(innerTuple.get(1), true);
            innerTuple = (Tuple) ((Tuple) t.get(5)).get(0);
            Assert.assertEquals(innerTuple.get(0), "23.4567");
            Assert.assertEquals(innerTuple.get(1), "23.4568");
            innerTuple = (Tuple) ((Tuple) t.get(6)).get(0);
            Assert.assertEquals(innerTuple.get(0), 12345678.12345678d);
            Assert.assertEquals(innerTuple.get(1), 12345678.12345679d);
            innerTuple = (Tuple) ((Tuple) t.get(7)).get(0);
            Assert.assertEquals(innerTuple.get(0), 123.12f);
            Assert.assertEquals(innerTuple.get(1), 123.13f);
            innerTuple = (Tuple) ((Tuple) t.get(8)).get(0);
            Assert.assertEquals(innerTuple.get(0), "127.0.0.1");
            Assert.assertEquals(innerTuple.get(1), "127.0.0.2");
            innerTuple = (Tuple) ((Tuple) t.get(9)).get(0);
            Assert.assertEquals(innerTuple.get(0), 123);
            Assert.assertEquals(innerTuple.get(1), 124);
            innerTuple = (Tuple) ((Tuple) t.get(10)).get(0);
            Assert.assertEquals(innerTuple.get(0), "text1");
            Assert.assertEquals(innerTuple.get(1), "text2");
            innerTuple = (Tuple) ((Tuple) t.get(11)).get(0);
            Assert.assertEquals(innerTuple.get(0), 1296705900000L);
            Assert.assertEquals(innerTuple.get(1), 1296792300000L);
            innerTuple = (Tuple) ((Tuple) t.get(12)).get(0);
            Assert.assertEquals(innerTuple.get(0), new DataByteArray((TimeUUIDType.instance.fromString("e23f450f-53a6-11e2-7f7f-7f7f7f7f7f7f").array())));
            Assert.assertEquals(innerTuple.get(1), new DataByteArray((TimeUUIDType.instance.fromString("e23f450f-53a6-11e2-7f7f-7f7f7f7f7f77").array())));
            innerTuple = (Tuple) ((Tuple) t.get(13)).get(0);
            Assert.assertEquals(innerTuple.get(0), new DataByteArray((UUIDType.instance.fromString("550e8400-e29b-41d4-a716-446655440000").array())));
            Assert.assertEquals(innerTuple.get(1), new DataByteArray((UUIDType.instance.fromString("550e8400-e29b-41d4-a716-446655440001").array())));
            innerTuple = (Tuple) ((Tuple) t.get(14)).get(0);
            Assert.assertEquals(innerTuple.get(0), "varchar1");
            Assert.assertEquals(innerTuple.get(1), "varchar2");  
            innerTuple = (Tuple) ((Tuple) t.get(15)).get(0);
            Assert.assertEquals(innerTuple.get(0), 123);
            Assert.assertEquals(innerTuple.get(1), 124);
        }
        else
        {
            Assert.fail("Failed to get data for query " + initialQuery);
        }
    }

}
