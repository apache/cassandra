/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.pig;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.Iterator;

import org.apache.cassandra.cli.CliMain;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.thrift.AuthenticationException;
import org.apache.cassandra.thrift.AuthorizationException;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class ThriftColumnFamilyTest extends PigTestBase
{    
    private static String[] statements = {
            "create keyspace thriftKs with placement_strategy = 'org.apache.cassandra.locator.SimpleStrategy' and" +
            " strategy_options={replication_factor:1};",
            "use thriftKs;",

            "create column family SomeApp " +
                    " with comparator = UTF8Type " +
                    " and default_validation_class = UTF8Type " +
                    " and key_validation_class = UTF8Type " +
                    " and column_metadata = [{column_name: name, validation_class: UTF8Type, index_type: KEYS}, " +
                    "{column_name: vote_type, validation_class: UTF8Type}, " +
                    "{column_name: rating, validation_class: Int32Type}, " +
                    "{column_name: score, validation_class: LongType}, " +
                    "{column_name: percent, validation_class: FloatType}, " +
                    "{column_name: atomic_weight, validation_class: DoubleType}, " +
                    "{column_name: created, validation_class: DateType},]; ",

             "create column family CopyOfSomeApp " +
                    "with key_validation_class = UTF8Type " +
                    "and default_validation_class = UTF8Type " +
                    "and comparator = UTF8Type " +
                    "and column_metadata = " +
                    "[ " +
                        "{column_name: name, validation_class: UTF8Type, index_type: KEYS}, " +
                        "{column_name: vote_type, validation_class: UTF8Type}, " +
                        "{column_name: rating, validation_class: Int32Type}, " +
                        "{column_name: score, validation_class: LongType}, " +
                        "{column_name: percent, validation_class: FloatType}, " +
                        "{column_name: atomic_weight, validation_class: DoubleType}, " +
                        "{column_name: created, validation_class: DateType}, " +
                    "];",

             "set SomeApp['foo']['name'] = 'User Foo';",
             "set SomeApp['foo']['vote_type'] = 'like';",
             "set SomeApp['foo']['rating'] = 8;",
             "set SomeApp['foo']['score'] = 125000;",
             "set SomeApp['foo']['percent'] = '85.0';",
             "set SomeApp['foo']['atomic_weight'] = '2.7182818284590451';",
             "set SomeApp['foo']['created'] = 1335890877;",

             "set SomeApp['bar']['name'] = 'User Bar';",
             "set SomeApp['bar']['vote_type'] = 'like';",
             "set SomeApp['bar']['rating'] = 9;",
             "set SomeApp['bar']['score'] = 15000;",
             "set SomeApp['bar']['percent'] = '35.0';",
             "set SomeApp['bar']['atomic_weight'] = '3.1415926535897931';",
             "set SomeApp['bar']['created'] = 1335890877;",

             "set SomeApp['baz']['name'] = 'User Baz';",
             "set SomeApp['baz']['vote_type'] = 'dislike';",
             "set SomeApp['baz']['rating'] = 3;",
             "set SomeApp['baz']['score'] = 512000;",
             "set SomeApp['baz']['percent'] = '95.3';",
             "set SomeApp['baz']['atomic_weight'] = '1.61803399';",
             "set SomeApp['baz']['created'] = 1335890877;",
             "set SomeApp['baz']['extra1'] = 'extra1';",
             "set SomeApp['baz']['extra2'] = 'extra2';",
             "set SomeApp['baz']['extra3'] = 'extra3';",

             "set SomeApp['qux']['name'] = 'User Qux';",
             "set SomeApp['qux']['vote_type'] = 'dislike';",
             "set SomeApp['qux']['rating'] = 2;",
             "set SomeApp['qux']['score'] = 12000;",
             "set SomeApp['qux']['percent'] = '64.7';",
             "set SomeApp['qux']['atomic_weight'] = '0.660161815846869';",
             "set SomeApp['qux']['created'] = 1335890877;",
             "set SomeApp['qux']['extra1'] = 'extra1';",
             "set SomeApp['qux']['extra2'] = 'extra2';",
             "set SomeApp['qux']['extra3'] = 'extra3';",
             "set SomeApp['qux']['extra4'] = 'extra4';",
             "set SomeApp['qux']['extra5'] = 'extra5';",
             "set SomeApp['qux']['extra6'] = 'extra6';",
             "set SomeApp['qux']['extra7'] = 'extra7';",

             "create column family U8 with " +
                     "key_validation_class = UTF8Type and " +
                     "comparator = UTF8Type;",
                     
             "create column family Bytes with " +
                      "key_validation_class = BytesType and " +
                      "comparator = UTF8Type;",

             "set U8['foo']['x'] = ascii('Z');",
             "set Bytes[ascii('foo')]['x'] = ascii('Z');",

             "create column family CC with " +
                       "key_validation_class = UTF8Type and " +
                       "default_validation_class=CounterColumnType " +
                       "and comparator=UTF8Type;",

             "incr CC['chuck']['kick'];",
             "incr CC['chuck']['kick'];",
             "incr CC['chuck']['kick'];",
             "incr CC['chuck']['fist'];",

             "create column family Compo " +
                       "with key_validation_class = UTF8Type " +
                       "and default_validation_class = UTF8Type " +
                       "and comparator = 'CompositeType(UTF8Type,UTF8Type)';",

             "set Compo['punch']['bruce:lee'] = 'ouch';",
             "set Compo['punch']['bruce:bruce'] = 'hunh?';",
             "set Compo['kick']['bruce:lee'] = 'oww';",
             "set Compo['kick']['bruce:bruce'] = 'watch it, mate';",

             "create column family CompoInt " +
                       "with key_validation_class = UTF8Type " +
                       "and default_validation_class = UTF8Type " +
                       "and comparator = 'CompositeType(LongType,LongType)';",

            "set CompoInt['clock']['1:0'] = 'z';",
            "set CompoInt['clock']['1:30'] = 'zzzz';",
            "set CompoInt['clock']['2:30'] = 'daddy?';",
            "set CompoInt['clock']['6:30'] = 'coffee...';",

            "create column family CompoIntCopy " +
                        "with key_validation_class = UTF8Type " +
                        "and default_validation_class = UTF8Type " +
                        "and comparator = 'CompositeType(LongType,LongType)';",

            "create column family CompoKey " +
                        "with key_validation_class = 'CompositeType(UTF8Type,LongType)' " +
                        "and default_validation_class = UTF8Type " +
                        "and comparator = LongType;",

            "set CompoKey['clock:10']['1'] = 'z';",
            "set CompoKey['clock:20']['1'] = 'zzzz';",
            "set CompoKey['clock:30']['2'] = 'daddy?';",
            "set CompoKey['clock:40']['6'] = 'coffee...';",

            "create column family CompoKeyCopy " +
                        "with key_validation_class = 'CompositeType(UTF8Type,LongType)' " +
                        "and default_validation_class = UTF8Type " +
                        "and comparator = LongType;"
    };

    private static String[] deleteCopyOfSomeAppTableData = { "use thriftKs;",
            "DEL CopyOfSomeApp ['foo']",
            "DEL CopyOfSomeApp ['bar']",
            "DEL CopyOfSomeApp ['baz']",
            "DEL CopyOfSomeApp ['qux']"
    };

    @BeforeClass
    public static void setup() throws TTransportException, IOException, InterruptedException, ConfigurationException,
                                      AuthenticationException, AuthorizationException, InvalidRequestException, UnavailableException, TimedOutException, TException, NotFoundException, CharacterCodingException, ClassNotFoundException, NoSuchFieldException, IllegalAccessException, InstantiationException
    {
        startCassandra();
        setupDataByCli(statements);
        startHadoopCluster();
    }

    @Test
    public void testCqlNativeStorage() throws IOException, ClassNotFoundException, TException, TimedOutException, NotFoundException, InvalidRequestException, NoSuchFieldException, UnavailableException, IllegalAccessException, InstantiationException, AuthenticationException, AuthorizationException
    {
        //regular thrift column families
        //input_cql=select * from "SomeApp" where token(key) > ? and token(key) <= ?
        cqlStorageTest("data = load 'cql://thriftKs/SomeApp?" + defaultParameters + nativeParameters + "&input_cql=select%20*%20from%20%22SomeApp%22%20where%20token(key)%20%3E%20%3F%20and%20token(key)%20%3C%3D%20%3F' using CqlNativeStorage();");

        //Test counter colun family
        //input_cql=select * from "CC" where token(key) > ? and token(key) <= ?
        cqlStorageCounterTableTest("cc_data = load 'cql://thriftKs/CC?" + defaultParameters + nativeParameters + "&input_cql=select%20*%20from%20%22CC%22%20where%20token(key)%20%3E%20%3F%20and%20token(key)%20%3C%3D%20%3F' using CqlNativeStorage();");

        //Test composite column family
        //input_cql=select * from "Compo" where token(key) > ? and token(key) <= ?
        cqlStorageCompositeTableTest("compo_data = load 'cql://thriftKs/Compo?" + defaultParameters + nativeParameters + "&input_cql=select%20*%20from%20%22Compo%22%20where%20token(key)%20%3E%20%3F%20and%20token(key)%20%3C%3D%20%3F' using CqlNativeStorage();");
    }

    private void cqlStorageTest(String initialQuery) throws IOException
    {
        pig.registerQuery(initialQuery);

        //(bar,3.141592653589793,1335890877,User Bar,35.0,9,15000,like)
        //(baz,1.61803399,1335890877,User Baz,95.3,3,512000,dislike)
        //(foo,2.718281828459045,1335890877,User Foo,85.0,8,125000,like)
        //(qux,0.660161815846869,1335890877,User Qux,64.7,2,12000,dislike)

        //{key: chararray,atomic_weight: double,created: long,name: chararray,percent: float,rating: int,score: long,vote_type: chararray}
        Iterator<Tuple> it = pig.openIterator("data");
        int count = 0;
        while (it.hasNext()) {
            count ++;
            Tuple t = it.next();
            if ("bar".equals(t.get(0)))
            {
                Assert.assertEquals(t.get(1), 3.141592653589793d);
                Assert.assertEquals(t.get(3), "User Bar");
                Assert.assertEquals(t.get(4), 35.0f);
                Assert.assertEquals(t.get(5), 9);
                Assert.assertEquals(t.get(6), 15000L);
                Assert.assertEquals(t.get(7), "like");
            }
            else if ("baz".equals(t.get(0)))
            {
                Assert.assertEquals(t.get(1), 1.61803399d);
                Assert.assertEquals(t.get(3), "User Baz");
                Assert.assertEquals(t.get(4), 95.3f);
                Assert.assertEquals(t.get(5), 3);
                Assert.assertEquals(t.get(6), 512000L);
                Assert.assertEquals(t.get(7), "dislike");
            }
            else if ("foo".equals(t.get(0)))
            {
                Assert.assertEquals(t.get(0), "foo");
                Assert.assertEquals(t.get(1), 2.718281828459045d);
                Assert.assertEquals(t.get(3), "User Foo");
                Assert.assertEquals(t.get(4), 85.0f);
                Assert.assertEquals(t.get(5), 8);
                Assert.assertEquals(t.get(6), 125000L);
                Assert.assertEquals(t.get(7), "like");
            }
            else if ("qux".equals(t.get(0)))
            {
                Assert.assertEquals(t.get(0), "qux");
                Assert.assertEquals(t.get(1), 0.660161815846869d);
                Assert.assertEquals(t.get(3), "User Qux");
                Assert.assertEquals(t.get(4), 64.7f);
                Assert.assertEquals(t.get(5), 2);
                Assert.assertEquals(t.get(6), 12000L);
                Assert.assertEquals(t.get(7), "dislike");
            }
        }
        Assert.assertEquals(count, 4);
    }

    private void cqlStorageCounterTableTest(String initialQuery) throws IOException
    {
        pig.registerQuery(initialQuery);

        //(chuck,fist,1)
        //(chuck,kick,3)

        // {key: chararray,column1: chararray,value: long}
        Iterator<Tuple> it = pig.openIterator("cc_data");
        int count = 0;
        while (it.hasNext()) {
            count ++;
            Tuple t = it.next();
            if ("chuck".equals(t.get(0)) && "fist".equals(t.get(1)))
                Assert.assertEquals(t.get(2), 1L);
            else if ("chuck".equals(t.get(0)) && "kick".equals(t.get(1)))
                Assert.assertEquals(t.get(2), 3L);
        }
        Assert.assertEquals(count, 2);
    }

    private void cqlStorageCompositeTableTest(String initialQuery) throws IOException
    {
        pig.registerQuery(initialQuery);

        //(kick,bruce,bruce,watch it, mate)
        //(kick,bruce,lee,oww)
        //(punch,bruce,bruce,hunh?)
        //(punch,bruce,lee,ouch)

        //{key: chararray,column1: chararray,column2: chararray,value: chararray}
        Iterator<Tuple> it = pig.openIterator("compo_data");
        int count = 0;
        while (it.hasNext()) {
            count ++;
            Tuple t = it.next();
            if ("kick".equals(t.get(0)) && "bruce".equals(t.get(1)) && "bruce".equals(t.get(2)))
                Assert.assertEquals(t.get(3), "watch it, mate");
            else if ("kick".equals(t.get(0)) && "bruce".equals(t.get(1)) && "lee".equals(t.get(2)))
                Assert.assertEquals(t.get(3), "oww");
            else if ("punch".equals(t.get(0)) && "bruce".equals(t.get(1)) && "bruce".equals(t.get(2)))
                Assert.assertEquals(t.get(3), "hunh?");
            else if ("punch".equals(t.get(0)) && "bruce".equals(t.get(1)) && "lee".equals(t.get(2)))
                Assert.assertEquals(t.get(3), "ouch");
        }
        Assert.assertEquals(count, 4);
    }

    @Test
    public void testCassandraStorageSchema() throws IOException, ClassNotFoundException, TException, TimedOutException, NotFoundException, InvalidRequestException, NoSuchFieldException, UnavailableException, IllegalAccessException, InstantiationException
    {
        //results: (qux,(atomic_weight,0.660161815846869),(created,1335890877),(name,User Qux),(percent,64.7),
        //(rating,2),(score,12000),(vote_type,dislike),{(extra1,extra1),
        //(extra2,extra2),(extra3,extra3),
        //(extra4,extra4),(extra5,extra5),
        //(extra6,extra6),(extra7,extra7)})
        pig.registerQuery("rows = LOAD 'cassandra://thriftKs/SomeApp?" + defaultParameters + "' USING CassandraStorage();");

        //schema: {key: chararray,atomic_weight: (name: chararray,value: double),created: (name: chararray,value: long),
        //name: (name: chararray,value: chararray),percent: (name: chararray,value: float),
        //rating: (name: chararray,value: int),score: (name: chararray,value: long),
        //vote_type: (name: chararray,value: chararray),columns: {(name: chararray,value: chararray)}}
        Iterator<Tuple> it = pig.openIterator("rows");
        if (it.hasNext()) {
            Tuple t = it.next();
            String rowKey =  t.get(0).toString();
            if ("qux".equals(rowKey))
            {
                Tuple column = (Tuple) t.get(1);
                Assert.assertEquals(column.get(0), "atomic_weight");
                Assert.assertEquals(column.get(1), 0.660161815846869d);
                column = (Tuple) t.get(3);
                Assert.assertEquals(column.get(0), "name");
                Assert.assertEquals(column.get(1), "User Qux");
                column = (Tuple) t.get(4);
                Assert.assertEquals(column.get(0), "percent");
                Assert.assertEquals(column.get(1), 64.7f);
                column = (Tuple) t.get(5);
                Assert.assertEquals(column.get(0), "rating");
                Assert.assertEquals(column.get(1), 2);
                column = (Tuple) t.get(6);
                Assert.assertEquals(column.get(0), "score");
                Assert.assertEquals(column.get(1), 12000L);
                column = (Tuple) t.get(7);
                Assert.assertEquals(column.get(0), "vote_type");
                Assert.assertEquals(column.get(1), "dislike");
                DataBag columns = (DataBag) t.get(8);
                Iterator<Tuple> iter = columns.iterator();
                int i = 0;
                while(iter.hasNext())
                {
                    i++;
                    column = iter.next();
                    Assert.assertEquals(column.get(0), "extra"+i);
                }
                Assert.assertEquals(7, columns.size());
            }

        }
    }

    @Test
    public void testCassandraStorageFullCopy() throws IOException, ClassNotFoundException, TException, TimedOutException, NotFoundException, InvalidRequestException, NoSuchFieldException, UnavailableException, IllegalAccessException, InstantiationException, AuthenticationException, AuthorizationException
    {
        pig.setBatchOn();
        pig.registerQuery("rows = LOAD 'cassandra://thriftKs/SomeApp?" + defaultParameters + "' USING CassandraStorage();");
        //full copy
        pig.registerQuery("STORE rows INTO 'cassandra://thriftKs/CopyOfSomeApp?" + defaultParameters + "' USING CassandraStorage();");
        pig.executeBatch();
        Assert.assertEquals("User Qux", getColumnValue("thriftKs", "CopyOfSomeApp", "name", "qux", "UTF8Type"));
        Assert.assertEquals("dislike", getColumnValue("thriftKs", "CopyOfSomeApp", "vote_type", "qux", "UTF8Type"));
        Assert.assertEquals("64.7", getColumnValue("thriftKs", "CopyOfSomeApp", "percent", "qux", "FloatType"));
    }

    @Test
    public void testCassandraStorageSigleTupleCopy() throws IOException, ClassNotFoundException, TException, TimedOutException, NotFoundException, InvalidRequestException, NoSuchFieldException, UnavailableException, IllegalAccessException, InstantiationException, AuthenticationException, AuthorizationException
    {
        executeCliStatements(deleteCopyOfSomeAppTableData);
        pig.setBatchOn();
        pig.registerQuery("rows = LOAD 'cassandra://thriftKs/SomeApp?" + defaultParameters + "' USING CassandraStorage();");
        //sigle tuple
        pig.registerQuery("onecol = FOREACH rows GENERATE key, percent;");
        pig.registerQuery("STORE onecol INTO 'cassandra://thriftKs/CopyOfSomeApp?" + defaultParameters + "' USING CassandraStorage();");
        pig.executeBatch();
        String value = null;
        try
        {
            value = getColumnValue("thriftKs", "CopyOfSomeApp", "name", "qux", "UTF8Type");
        }
        catch (NotFoundException e)
        {
            Assert.assertTrue(true);
        }
        if (value != null)
            Assert.fail();
        try
        {
            value = getColumnValue("thriftKs", "CopyOfSomeApp", "vote_type", "qux", "UTF8Type");
        }
        catch (NotFoundException e)
        {
            Assert.assertTrue(true);
        }
        if (value != null)
            Assert.fail();
        Assert.assertEquals("64.7", getColumnValue("thriftKs", "CopyOfSomeApp", "percent", "qux", "FloatType"));
    }

    @Test
    public void testCassandraStorageBagOnlyCopy() throws IOException, ClassNotFoundException, TException, TimedOutException, NotFoundException, InvalidRequestException, NoSuchFieldException, UnavailableException, IllegalAccessException, InstantiationException, AuthenticationException, AuthorizationException
    {
        executeCliStatements(deleteCopyOfSomeAppTableData);
        pig.setBatchOn();
        pig.registerQuery("rows = LOAD 'cassandra://thriftKs/SomeApp?" + defaultParameters + "' USING CassandraStorage();");
        //bag only
        pig.registerQuery("other = FOREACH rows GENERATE key, columns;");
        pig.registerQuery("STORE other INTO 'cassandra://thriftKs/CopyOfSomeApp?" + defaultParameters + "' USING CassandraStorage();");
        pig.executeBatch();
        String value = null;
        try
        {
            value = getColumnValue("thriftKs", "CopyOfSomeApp", "name", "qux", "UTF8Type");
        }
        catch (NotFoundException e)
        {
            Assert.assertTrue(true);
        }
        if (value != null)
            Assert.fail();
        try
        {
            value = getColumnValue("thriftKs", "CopyOfSomeApp", "vote_type", "qux", "UTF8Type");
        }
        catch (NotFoundException e)
        {
            Assert.assertTrue(true);
        }
        if (value != null)
            Assert.fail();
        try
        {
            value = getColumnValue("thriftKs", "CopyOfSomeApp", "percent", "qux", "FloatType");
        }
        catch (NotFoundException e)
        {
            Assert.assertTrue(true);
        }
        if (value != null)
            Assert.fail();
        Assert.assertEquals("extra1", getColumnValue("thriftKs", "CopyOfSomeApp", "extra1", "qux", "UTF8Type"));
    }

    @Test
    public void testCassandraStorageFilter() throws IOException, ClassNotFoundException, TException, TimedOutException, NotFoundException, InvalidRequestException, NoSuchFieldException, UnavailableException, IllegalAccessException, InstantiationException, AuthenticationException, AuthorizationException
    {
        executeCliStatements(deleteCopyOfSomeAppTableData);
        pig.setBatchOn();
        pig.registerQuery("rows = LOAD 'cassandra://thriftKs/SomeApp?" + defaultParameters + "' USING CassandraStorage();");

        //filter
        pig.registerQuery("likes = FILTER rows by vote_type.value eq 'like' and rating.value > 5;");
        pig.registerQuery("STORE likes INTO 'cassandra://thriftKs/CopyOfSomeApp?" + defaultParameters + "' USING CassandraStorage();");
        pig.executeBatch();

        Assert.assertEquals("like", getColumnValue("thriftKs", "CopyOfSomeApp", "vote_type", "bar", "UTF8Type"));
        Assert.assertEquals("like", getColumnValue("thriftKs", "CopyOfSomeApp", "vote_type", "foo", "UTF8Type"));
        String value = null;
        try
        {
            value = getColumnValue("thriftKs", "CopyOfSomeApp", "vote_type", "qux", "UTF8Type");
        }
        catch (NotFoundException e)
        {
            Assert.assertTrue(true);
        }
        if (value != null)
            Assert.fail();
        try
        {
            value = getColumnValue("thriftKs", "CopyOfSomeApp", "vote_type", "baz", "UTF8Type");
        }
        catch (NotFoundException e)
        {
            Assert.assertTrue(true);
        }
        if (value != null)
            Assert.fail();

        executeCliStatements(deleteCopyOfSomeAppTableData);
        pig.setBatchOn();
        pig.registerQuery("rows = LOAD 'cassandra://thriftKs/SomeApp?" + defaultParameters + "' USING CassandraStorage();");
        pig.registerQuery("dislikes_extras = FILTER rows by vote_type.value eq 'dislike' AND COUNT(columns) > 0;");
        pig.registerQuery("STORE dislikes_extras INTO 'cassandra://thriftKs/CopyOfSomeApp?" + defaultParameters + "' USING CassandraStorage();");
        pig.registerQuery("visible = FILTER rows BY COUNT(columns) == 0;");
        pig.executeBatch();
        Assert.assertEquals("dislike", getColumnValue("thriftKs", "CopyOfSomeApp", "vote_type", "baz", "UTF8Type"));
        Assert.assertEquals("dislike", getColumnValue("thriftKs", "CopyOfSomeApp", "vote_type", "qux", "UTF8Type"));
        value = null;
        try
        {
            value = getColumnValue("thriftKs", "CopyOfSomeApp", "vote_type", "bar", "UTF8Type");
        }
        catch (NotFoundException e)
        {
            Assert.assertTrue(true);
        }
        if (value != null)
            Assert.fail();
        try
        {
            value = getColumnValue("thriftKs", "CopyOfSomeApp", "vote_type", "foo", "UTF8Type");
        }
        catch (NotFoundException e)
        {
            Assert.assertTrue(true);
        }
        if (value != null)
            Assert.fail();
    }

    @Test
    public void testCassandraStorageJoin() throws IOException, ClassNotFoundException, TException, TimedOutException, NotFoundException, InvalidRequestException, NoSuchFieldException, UnavailableException, IllegalAccessException, InstantiationException, AuthenticationException, AuthorizationException
    {
        //test key types with a join
        pig.registerQuery("U8 = load 'cassandra://thriftKs/U8?" + defaultParameters + "' using CassandraStorage();");
        pig.registerQuery("Bytes = load 'cassandra://thriftKs/Bytes?" + defaultParameters + "' using CassandraStorage();");

        //cast key to chararray
        pig.registerQuery("b = foreach Bytes generate (chararray)key, columns;");

        //key in Bytes is a bytearray, U8 chararray
        //(foo,{(x,Z)},foo,{(x,Z)})
        pig.registerQuery("a = join Bytes by key, U8 by key;");
        Iterator<Tuple> it = pig.openIterator("a");
        if (it.hasNext()) {
            Tuple t = it.next();
            Assert.assertEquals(t.get(0), new DataByteArray("foo".getBytes()));
            DataBag columns = (DataBag) t.get(1);
            Iterator<Tuple> iter = columns.iterator();
            Tuple t1 = iter.next();
            Assert.assertEquals(t1.get(0), "x");
            Assert.assertEquals(t1.get(1), new DataByteArray("Z".getBytes()));
            String column = (String) t.get(2);
            Assert.assertEquals(column, "foo");
            columns = (DataBag) t.get(3);
            iter = columns.iterator();
            Tuple t2 = iter.next();
            Assert.assertEquals(t2.get(0), "x");
            Assert.assertEquals(t2.get(1), new DataByteArray("Z".getBytes()));
        }
        //key should now be cast into a chararray
        //(foo,{(x,Z)},foo,{(x,Z)})
        pig.registerQuery("c = join b by (chararray)key, U8 by (chararray)key;");
        it = pig.openIterator("c");
        if (it.hasNext()) {
            Tuple t = it.next();
            Assert.assertEquals(t.get(0), "foo");
            DataBag columns = (DataBag) t.get(1);
            Iterator<Tuple> iter = columns.iterator();
            Tuple t1 = iter.next();
            Assert.assertEquals(t1.get(0), "x");
            Assert.assertEquals(t1.get(1), new DataByteArray("Z".getBytes()));
            String column = (String) t.get(2);
            Assert.assertEquals(column, "foo");
            columns = (DataBag) t.get(3);
            iter = columns.iterator();
            Tuple t2 = iter.next();
            Assert.assertEquals(t2.get(0), "x");
            Assert.assertEquals(t2.get(1), new DataByteArray("Z".getBytes()));
        }
    }

    @Test
    public void testCassandraStorageCounterCF() throws IOException, ClassNotFoundException, TException, TimedOutException, NotFoundException, InvalidRequestException, NoSuchFieldException, UnavailableException, IllegalAccessException, InstantiationException, AuthenticationException, AuthorizationException
    {
        pig.registerQuery("rows = LOAD 'cassandra://thriftKs/SomeApp?" + defaultParameters + "' USING CassandraStorage();");

        //Test counter column family support
        pig.registerQuery("CC = load 'cassandra://thriftKs/CC?" + defaultParameters + "' using CassandraStorage();");
        pig.registerQuery("total_hits = foreach CC generate key, SUM(columns.value);");
        //(chuck,4)
        Iterator<Tuple> it = pig.openIterator("total_hits");
        if (it.hasNext()) {
            Tuple t = it.next();
            Assert.assertEquals(t.get(0), "chuck");
            Assert.assertEquals(t.get(1), 4l);
        }
    }

    /** This test case fails due to antlr lib conflicts, Cassandra2.1 uses 3.2, Hive1.2 uses 3.4 */
    //@Test
    public void testCassandraStorageCompositeColumnCF() throws IOException, ClassNotFoundException, TException, TimedOutException, NotFoundException, InvalidRequestException, NoSuchFieldException, UnavailableException, IllegalAccessException, InstantiationException, AuthenticationException, AuthorizationException
    {
        //Test CompositeType
        pig.registerQuery("compo = load 'cassandra://thriftKs/Compo?" + defaultParameters + "' using CassandraStorage();");
        pig.registerQuery("compo = foreach compo generate key as method, flatten(columns);");
        pig.registerQuery("lee = filter compo by columns::name == ('bruce','lee');");

        //(kick,(bruce,lee),oww)
        //(punch,(bruce,lee),ouch)
        Iterator<Tuple> it = pig.openIterator("lee");
        int count = 0;
        while (it.hasNext()) {
            count ++;
            Tuple t = it.next();
            Tuple t1 = (Tuple) t.get(1);
            Assert.assertEquals(t1.get(0), "bruce");
            Assert.assertEquals(t1.get(1), "lee");
            if ("kick".equals(t.get(0)))
                Assert.assertEquals(t.get(2), "oww");
            else if ("kick".equals(t.get(0)))
                Assert.assertEquals(t.get(2), "ouch");
        }
        Assert.assertEquals(count, 2);
        pig.registerQuery("night = load 'cassandra://thriftKs/CompoInt?" + defaultParameters + "' using CassandraStorage();");
        pig.registerQuery("night = foreach night generate flatten(columns);");
        pig.registerQuery("night = foreach night generate (int)columns::name.$0+(double)columns::name.$1/60 as hour, columns::value as noise;");

        //What happens at the darkest hour?
        pig.registerQuery("darkest = filter night by hour > 2 and hour < 5;");

        //(2.5,daddy?)
        it = pig.openIterator("darkest");
        if (it.hasNext()) {
            Tuple t = it.next();
            Assert.assertEquals(t.get(0), 2.5d);
            Assert.assertEquals(t.get(1), "daddy?");
        }
        pig.setBatchOn();
        pig.registerQuery("compo_int_rows = LOAD 'cassandra://thriftKs/CompoInt?" + defaultParameters + "' using CassandraStorage();");
        pig.registerQuery("STORE compo_int_rows INTO 'cassandra://thriftKs/CompoIntCopy?" + defaultParameters + "' using CassandraStorage();");
        pig.executeBatch();
        pig.registerQuery("compocopy_int_rows = LOAD 'cassandra://thriftKs/CompoIntCopy?" + defaultParameters + "' using CassandraStorage();");
        //(clock,{((1,0),z),((1,30),zzzz),((2,30),daddy?),((6,30),coffee...)})
        it = pig.openIterator("compocopy_int_rows");
        count = 0;
        if (it.hasNext()) {
            Tuple t = it.next();
            Assert.assertEquals(t.get(0), "clock");
            DataBag columns = (DataBag) t.get(1);
            Iterator<Tuple> iter = columns.iterator();
            while (iter.hasNext())
            {
                count ++;
                Tuple t1 = iter.next();
                Tuple inner = (Tuple) t1.get(0);
                if ((Long) inner.get(0) == 1L && (Long) inner.get(1) == 0L)
                    Assert.assertEquals(t1.get(1), "z");
                else if ((Long) inner.get(0) == 1L && (Long) inner.get(1) == 30L)
                    Assert.assertEquals(t1.get(1), "zzzz");
                else if ((Long) inner.get(0) == 2L && (Long) inner.get(1) == 30L)
                    Assert.assertEquals(t1.get(1), "daddy?");
                else if ((Long) inner.get(0) == 6L && (Long) inner.get(1) == 30L)
                    Assert.assertEquals(t1.get(1), "coffee...");
            }
            Assert.assertEquals(count, 4);
        }
    }

    @Test
    public void testCassandraStorageCompositeKeyCF() throws IOException, ClassNotFoundException, TException, TimedOutException, NotFoundException, InvalidRequestException, NoSuchFieldException, UnavailableException, IllegalAccessException, InstantiationException, AuthenticationException, AuthorizationException
    {
        //Test CompositeKey
        pig.registerQuery("compokeys = load 'cassandra://thriftKs/CompoKey?" + defaultParameters + "' using CassandraStorage();");
        pig.registerQuery("compokeys = filter compokeys by key.$1 == 40;");
        //((clock,40),{(6,coffee...)})
        Iterator<Tuple> it = pig.openIterator("compokeys");
        if (it.hasNext()) {
            Tuple t = it.next();
            Tuple key = (Tuple) t.get(0); 
            Assert.assertEquals(key.get(0), "clock");
            Assert.assertEquals(key.get(1), 40L);
            DataBag columns = (DataBag) t.get(1);
            Iterator<Tuple> iter = columns.iterator();
            if (iter.hasNext())
            {
                Tuple t1 = iter.next();
                Assert.assertEquals(t1.get(0), 6L);
                Assert.assertEquals(t1.get(1), "coffee...");
            }
        }
        pig.setBatchOn();
        pig.registerQuery("compo_key_rows = LOAD 'cassandra://thriftKs/CompoKey?" + defaultParameters + "' using CassandraStorage();");
        pig.registerQuery("STORE compo_key_rows INTO 'cassandra://thriftKs/CompoKeyCopy?" + defaultParameters + "' using CassandraStorage();");
        pig.executeBatch();
        pig.registerQuery("compo_key_copy_rows = LOAD 'cassandra://thriftKs/CompoKeyCopy?" + defaultParameters + "' using CassandraStorage();");
        //((clock,10),{(1,z)})
        //((clock,20),{(1,zzzz)})
        //((clock,30),{(2,daddy?)})
        //((clock,40),{(6,coffee...)})
        it = pig.openIterator("compo_key_copy_rows");
        int count = 0;
        while (it.hasNext()) {
            Tuple t = it.next();
            count ++;
            Tuple key = (Tuple) t.get(0); 
            if ("clock".equals(key.get(0)) && (Long) key.get(1) == 10L)
            {
                DataBag columns = (DataBag) t.get(1);
                Iterator<Tuple> iter = columns.iterator();
                if (iter.hasNext())
                {
                    Tuple t1 = iter.next();
                    Assert.assertEquals(t1.get(0), 1L);
                    Assert.assertEquals(t1.get(1), "z");
                }
            }
            else if ("clock".equals(key.get(0)) && (Long) key.get(1) == 40L)
            {
                DataBag columns = (DataBag) t.get(1);
                Iterator<Tuple> iter = columns.iterator();
                if (iter.hasNext())
                {
                    Tuple t1 = iter.next();
                    Assert.assertEquals(t1.get(0), 6L);
                    Assert.assertEquals(t1.get(1), "coffee...");
                }
            }
            else if ("clock".equals(key.get(0)) && (Long) key.get(1) == 20L)
            {
                DataBag columns = (DataBag) t.get(1);
                Iterator<Tuple> iter = columns.iterator();
                if (iter.hasNext())
                {
                    Tuple t1 = iter.next();
                    Assert.assertEquals(t1.get(0), 1L);
                    Assert.assertEquals(t1.get(1), "zzzz");
                }
            }
            else if ("clock".equals(key.get(0)) && (Long) key.get(1) == 30L)
            {
                DataBag columns = (DataBag) t.get(1);
                Iterator<Tuple> iter = columns.iterator();
                if (iter.hasNext())
                {
                    Tuple t1 = iter.next();
                    Assert.assertEquals(t1.get(0), 2L);
                    Assert.assertEquals(t1.get(1), "daddy?");
                }
            }
        }
        Assert.assertEquals(count, 4);
    }

    private String getColumnValue(String ks, String cf, String colName, String key, String validator)
    throws AuthenticationException, AuthorizationException, InvalidRequestException, UnavailableException, TimedOutException, TException, NotFoundException, IOException
    {
        Cassandra.Client client = getClient();
        client.set_keyspace(ks);

        ByteBuffer key_user_id = ByteBufferUtil.bytes(key);
        ColumnPath cp = new ColumnPath(cf);
        cp.column = ByteBufferUtil.bytes(colName);

        // read
        ColumnOrSuperColumn got = client.get(key_user_id, cp, ConsistencyLevel.ONE);
        return parseType(validator).getString(got.getColumn().value);
    }

    private void executeCliStatements(String[] statements) throws CharacterCodingException, ClassNotFoundException, TException, TimedOutException, NotFoundException, InvalidRequestException, NoSuchFieldException, UnavailableException, IllegalAccessException, InstantiationException
    {
        CliMain.connect("127.0.0.1", 9170);
        try
        {
            for (String stmt : statements)
                CliMain.processStatement(stmt);
        }
        catch (Exception e)
        {
        }
    }
}
