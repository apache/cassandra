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
import java.util.Iterator;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.thrift.TException;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class ThriftColumnFamilyTest extends PigTestBase
{    
    private static String[] statements = {
            "DROP KEYSPACE IF EXISTS thrift_ks",
            "CREATE KEYSPACE thrift_ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};",
            "USE thrift_ks;",

            "CREATE TABLE some_app (" +
            "key text PRIMARY KEY," +
            "name text," +
            "vote_type text," +
            "rating int," +
            "score bigint," +
            "percent float," +
            "atomic_weight double," +
            "created timestamp)" +
            " WITH COMPACT STORAGE;",

            "CREATE INDEX ON some_app(name);",

            "INSERT INTO some_app (key, name, vote_type, rating, score, percent, atomic_weight, created) " +
                    "VALUES ('foo', 'User Foo', 'like', 8, 125000, 85.0, 2.7182818284590451, 1335890877);",

            "INSERT INTO some_app (key, name, vote_type, rating, score, percent, atomic_weight, created) " +
                    "VALUES ('bar', 'User Bar', 'like', 9, 15000, 35.0, 3.1415926535897931, 1335890877);",

            "INSERT INTO some_app (key, name, vote_type, rating, score, percent, atomic_weight, created) " +
                    "VALUES ('baz', 'User Baz', 'dislike', 3, 512000, 95.3, 1.61803399, 1335890877);",

            "INSERT INTO some_app (key, name, vote_type, rating, score, percent, atomic_weight, created) " +
                    "VALUES ('qux', 'User Qux', 'dislike', 2, 12000, 64.7, 0.660161815846869, 1335890877);",

            "CREATE TABLE copy_of_some_app (" +
            "key text PRIMARY KEY," +
            "name text," +
            "vote_type text," +
            "rating int," +
            "score bigint," +
            "percent float," +
            "atomic_weight double," +
            "created timestamp)" +
            " WITH COMPACT STORAGE;",

            "CREATE INDEX ON copy_of_some_app(name);",

            "CREATE TABLE u8 (" +
            "key text," +
            "column1 text," +
            "value blob," +
            "PRIMARY KEY (key, column1))" +
            " WITH COMPACT STORAGE",

            "INSERT INTO u8 (key, column1, value) VALUES ('foo', 'x', asciiAsBlob('Z'))",

            "CREATE TABLE bytes (" +
            "key blob," +
            "column1 text," +
            "value blob," +
            "PRIMARY KEY (key, column1))" +
            " WITH COMPACT STORAGE",

            "INSERT INTO bytes (key, column1, value) VALUES (asciiAsBlob('foo'), 'x', asciiAsBlob('Z'))",

            "CREATE TABLE cc (key text, name text, value counter, PRIMARY KEY (key, name)) WITH COMPACT STORAGE",

            "UPDATE cc SET value = value + 3 WHERE key = 'chuck' AND name = 'kick'",
            "UPDATE cc SET value = value + 1 WHERE key = 'chuck' AND name = 'fist'",

            "CREATE TABLE compo (" +
            "key text," +
            "column1 text," +
            "column2 text," +
            "value text," +
            "PRIMARY KEY (key, column1, column2))" +
            " WITH COMPACT STORAGE",

            "INSERT INTO compo (key, column1, column2, value) VALUES ('punch', 'bruce', 'lee', 'ouch');",
            "INSERT INTO compo (key, column1, column2, value) VALUES ('punch', 'bruce', 'bruce', 'hunh?');",
            "INSERT INTO compo (key, column1, column2, value) VALUES ('kick', 'bruce', 'lee', 'oww');",
            "INSERT INTO compo (key, column1, column2, value) VALUES ('kick', 'bruce', 'bruce', 'watch it, mate');",

            "CREATE TABLE compo_int (" +
            "key text," +
            "column1 bigint," +
            "column2 bigint," +
            "value text," +
            "PRIMARY KEY (key, column1, column2))" +
            " WITH COMPACT STORAGE",

            "INSERT INTO compo_int (key, column1, column2, value) VALUES ('clock', 1, 0, 'z');",
            "INSERT INTO compo_int (key, column1, column2, value) VALUES ('clock', 1, 30, 'zzzz');",
            "INSERT INTO compo_int (key, column1, column2, value) VALUES ('clock', 2, 30, 'daddy?');",
            "INSERT INTO compo_int (key, column1, column2, value) VALUES ('clock', 6, 30, 'coffee...');",

            "CREATE TABLE compo_int_copy (" +
            "key text," +
            "column1 bigint," +
            "column2 bigint," +
            "value text," +
            "PRIMARY KEY (key, column1, column2))" +
            " WITH COMPACT STORAGE",

            "CREATE TABLE compo_key (" +
            "key text," +
            "column1 bigint," +
            "column2 bigint," +
            "value text," +
            "PRIMARY KEY ((key, column1), column2))" +
            " WITH COMPACT STORAGE",

            "INSERT INTO compo_key (key, column1, column2, value) VALUES ('clock', 10, 1, 'z');",
            "INSERT INTO compo_key (key, column1, column2, value) VALUES ('clock', 20, 1, 'zzzz');",
            "INSERT INTO compo_key (key, column1, column2, value) VALUES ('clock', 30, 2, 'daddy?');",
            "INSERT INTO compo_key (key, column1, column2, value) VALUES ('clock', 40, 6, 'coffee...');",

            "CREATE TABLE compo_key_copy (" +
            "key text," +
            "column1 bigint," +
            "column2 bigint," +
            "value text," +
            "PRIMARY KEY ((key, column1), column2))" +
            " WITH COMPACT STORAGE",
    };

    private static String[] deleteCopyOfSomeAppTableData = {
            "use thrift_ks;",
            "DELETE FROM copy_of_some_app WHERE key = 'foo';",
            "DELETE FROM copy_of_some_app WHERE key = 'bar';",
            "DELETE FROM copy_of_some_app WHERE key = 'baz';",
            "DELETE FROM copy_of_some_app WHERE key = 'qux';",
    };

    @BeforeClass
    public static void setup() throws IOException, ConfigurationException, TException
    {
        startCassandra();
        executeCQLStatements(statements);
        startHadoopCluster();
    }

    @Test
    public void testCqlNativeStorage() throws IOException
    {
        //regular thrift column families
        //input_cql=select * from "some_app" where token(key) > ? and token(key) <= ?
        cqlStorageTest("data = load 'cql://thrift_ks/some_app?" + defaultParameters + nativeParameters + "&input_cql=select%20*%20from%20%22some_app%22%20where%20token(key)%20%3E%20%3F%20and%20token(key)%20%3C%3D%20%3F' using CqlNativeStorage();");

        //Test counter column family
        //input_cql=select * from "cc" where token(key) > ? and token(key) <= ?
        cqlStorageCounterTableTest("cc_data = load 'cql://thrift_ks/cc?" + defaultParameters + nativeParameters + "&input_cql=select%20*%20from%20%22cc%22%20where%20token(key)%20%3E%20%3F%20and%20token(key)%20%3C%3D%20%3F' using CqlNativeStorage();");

        //Test composite column family
        //input_cql=select * from "compo" where token(key) > ? and token(key) <= ?
        cqlStorageCompositeTableTest("compo_data = load 'cql://thrift_ks/compo?" + defaultParameters + nativeParameters + "&input_cql=select%20*%20from%20%22compo%22%20where%20token(key)%20%3E%20%3F%20and%20token(key)%20%3C%3D%20%3F' using CqlNativeStorage();");
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
    public void testCassandraStorageSchema() throws IOException
    {
        //results: (qux,(atomic_weight,0.660161815846869),(created,1335890877),(name,User Qux),(percent,64.7),
        //(rating,2),(score,12000),(vote_type,dislike))
        pig.registerQuery("rows = LOAD 'cassandra://thrift_ks/some_app?" + defaultParameters + "' USING CassandraStorage();");

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
            }
        }
    }

    @Test
    public void testCassandraStorageFullCopy() throws IOException, TException
    {
        pig.setBatchOn();
        pig.registerQuery("rows = LOAD 'cassandra://thrift_ks/some_app?" + defaultParameters + "' USING CassandraStorage();");
        //full copy
        pig.registerQuery("STORE rows INTO 'cassandra://thrift_ks/copy_of_some_app?" + defaultParameters + "' USING CassandraStorage();");
        pig.executeBatch();
        Assert.assertEquals("User Qux", getColumnValue("thrift_ks", "copy_of_some_app", "name", "qux", "UTF8Type"));
        Assert.assertEquals("dislike", getColumnValue("thrift_ks", "copy_of_some_app", "vote_type", "qux", "UTF8Type"));
        Assert.assertEquals("64.7", getColumnValue("thrift_ks", "copy_of_some_app", "percent", "qux", "FloatType"));
    }

    @Test
    public void testCassandraStorageSingleTupleCopy() throws IOException, TException
    {
        executeCQLStatements(deleteCopyOfSomeAppTableData);
        pig.setBatchOn();
        pig.registerQuery("rows = LOAD 'cassandra://thrift_ks/some_app?" + defaultParameters + "' USING CassandraStorage();");
        //single tuple
        pig.registerQuery("onecol = FOREACH rows GENERATE key, percent;");
        pig.registerQuery("STORE onecol INTO 'cassandra://thrift_ks/copy_of_some_app?" + defaultParameters + "' USING CassandraStorage();");
        pig.executeBatch();
        String value = null;
        try
        {
            value = getColumnValue("thrift_ks", "copy_of_some_app", "name", "qux", "UTF8Type");
        }
        catch (NotFoundException e)
        {
            Assert.assertTrue(true);
        }
        if (value != null)
            Assert.fail();
        try
        {
            value = getColumnValue("thrift_ks", "copy_of_some_app", "vote_type", "qux", "UTF8Type");
        }
        catch (NotFoundException e)
        {
            Assert.assertTrue(true);
        }
        if (value != null)
            Assert.fail();
        Assert.assertEquals("64.7", getColumnValue("thrift_ks", "copy_of_some_app", "percent", "qux", "FloatType"));
    }

    @Test
    public void testCassandraStorageBagOnlyCopy() throws IOException, TException
    {
        executeCQLStatements(deleteCopyOfSomeAppTableData);
        pig.setBatchOn();
        pig.registerQuery("rows = LOAD 'cassandra://thrift_ks/some_app?" + defaultParameters + "' USING CassandraStorage();");
        //bag only
        pig.registerQuery("other = FOREACH rows GENERATE key, columns;");
        pig.registerQuery("STORE other INTO 'cassandra://thrift_ks/copy_of_some_app?" + defaultParameters + "' USING CassandraStorage();");
        pig.executeBatch();
        String value = null;
        try
        {
            value = getColumnValue("thrift_ks", "copy_of_some_app", "name", "qux", "UTF8Type");
        }
        catch (NotFoundException e)
        {
            Assert.assertTrue(true);
        }
        if (value != null)
            Assert.fail();
        try
        {
            value = getColumnValue("thrift_ks", "copy_of_some_app", "vote_type", "qux", "UTF8Type");
        }
        catch (NotFoundException e)
        {
            Assert.assertTrue(true);
        }
        if (value != null)
            Assert.fail();
        try
        {
            value = getColumnValue("thrift_ks", "copy_of_some_app", "percent", "qux", "FloatType");
        }
        catch (NotFoundException e)
        {
            Assert.assertTrue(true);
        }
        if (value != null)
            Assert.fail();
    }

    @Test
    public void testCassandraStorageFilter() throws IOException, TException
    {
        executeCQLStatements(deleteCopyOfSomeAppTableData);
        pig.setBatchOn();
        pig.registerQuery("rows = LOAD 'cassandra://thrift_ks/some_app?" + defaultParameters + "' USING CassandraStorage();");

        //filter
        pig.registerQuery("likes = FILTER rows by vote_type.value eq 'like' and rating.value > 5;");
        pig.registerQuery("STORE likes INTO 'cassandra://thrift_ks/copy_of_some_app?" + defaultParameters + "' USING CassandraStorage();");
        pig.executeBatch();

        Assert.assertEquals("like", getColumnValue("thrift_ks", "copy_of_some_app", "vote_type", "bar", "UTF8Type"));
        Assert.assertEquals("like", getColumnValue("thrift_ks", "copy_of_some_app", "vote_type", "foo", "UTF8Type"));
        String value = null;
        try
        {
            value = getColumnValue("thrift_ks", "copy_of_some_app", "vote_type", "qux", "UTF8Type");
        }
        catch (NotFoundException e)
        {
            Assert.assertTrue(true);
        }
        if (value != null)
            Assert.fail();
        try
        {
            value = getColumnValue("thrift_ks", "copy_of_some_app", "vote_type", "baz", "UTF8Type");
        }
        catch (NotFoundException e)
        {
            Assert.assertTrue(true);
        }
        if (value != null)
            Assert.fail();

        executeCQLStatements(deleteCopyOfSomeAppTableData);
        pig.setBatchOn();
        pig.registerQuery("rows = LOAD 'cassandra://thrift_ks/some_app?" + defaultParameters + "' USING CassandraStorage();");
        pig.registerQuery("dislikes_extras = FILTER rows by vote_type.value eq 'dislike';");
        pig.registerQuery("STORE dislikes_extras INTO 'cassandra://thrift_ks/copy_of_some_app?" + defaultParameters + "' USING CassandraStorage();");
        pig.executeBatch();
        Assert.assertEquals("dislike", getColumnValue("thrift_ks", "copy_of_some_app", "vote_type", "baz", "UTF8Type"));
        Assert.assertEquals("dislike", getColumnValue("thrift_ks", "copy_of_some_app", "vote_type", "qux", "UTF8Type"));
        value = null;
        try
        {
            value = getColumnValue("thrift_ks", "copy_of_some_app", "vote_type", "bar", "UTF8Type");
        }
        catch (NotFoundException e)
        {
            Assert.assertTrue(true);
        }
        if (value != null)
            Assert.fail();
        try
        {
            value = getColumnValue("thrift_ks", "copy_of_some_app", "vote_type", "foo", "UTF8Type");
        }
        catch (NotFoundException e)
        {
            Assert.assertTrue(true);
        }
        if (value != null)
            Assert.fail();
    }

    @Test
    public void testCassandraStorageJoin() throws IOException
    {
        //test key types with a join
        pig.registerQuery("U8 = load 'cassandra://thrift_ks/u8?" + defaultParameters + "' using CassandraStorage();");
        pig.registerQuery("Bytes = load 'cassandra://thrift_ks/bytes?" + defaultParameters + "' using CassandraStorage();");

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
    public void testCassandraStorageCounterCF() throws IOException
    {
        //Test counter column family support
        pig.registerQuery("CC = load 'cassandra://thrift_ks/cc?" + defaultParameters + "' using CassandraStorage();");
        pig.registerQuery("total_hits = foreach CC generate key, SUM(columns.value);");
        //(chuck,4)
        Tuple t = pig.openIterator("total_hits").next();
        Assert.assertEquals(t.get(0), "chuck");
        Assert.assertEquals(t.get(1), 4l);
    }

    @Test
    public void testCassandraStorageCompositeColumnCF() throws IOException
    {
        //Test CompositeType
        pig.registerQuery("compo = load 'cassandra://thrift_ks/compo?" + defaultParameters + "' using CassandraStorage();");
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
        pig.registerQuery("night = load 'cassandra://thrift_ks/compo_int?" + defaultParameters + "' using CassandraStorage();");
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
        pig.registerQuery("compo_int_rows = LOAD 'cassandra://thrift_ks/compo_int?" + defaultParameters + "' using CassandraStorage();");
        pig.registerQuery("STORE compo_int_rows INTO 'cassandra://thrift_ks/compo_int_copy?" + defaultParameters + "' using CassandraStorage();");
        pig.executeBatch();
        pig.registerQuery("compocopy_int_rows = LOAD 'cassandra://thrift_ks/compo_int_copy?" + defaultParameters + "' using CassandraStorage();");
        //(clock,{((1,0),z),((1,30),zzzz),((2,30),daddy?),((6,30),coffee...)})
        it = pig.openIterator("compocopy_int_rows");
        count = 0;
        if (it.hasNext()) {
            Tuple t = it.next();
            Assert.assertEquals(t.get(0), "clock");
            DataBag columns = (DataBag) t.get(1);
            for (Tuple t1 : columns)
            {
                count++;
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
    public void testCassandraStorageCompositeKeyCF() throws IOException
    {
        //Test CompositeKey
        pig.registerQuery("compokeys = load 'cassandra://thrift_ks/compo_key?" + defaultParameters + "' using CassandraStorage();");
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
        pig.registerQuery("compo_key_rows = LOAD 'cassandra://thrift_ks/compo_key?" + defaultParameters + "' using CassandraStorage();");
        pig.registerQuery("STORE compo_key_rows INTO 'cassandra://thrift_ks/compo_key_copy?" + defaultParameters + "' using CassandraStorage();");
        pig.executeBatch();
        pig.registerQuery("compo_key_copy_rows = LOAD 'cassandra://thrift_ks/compo_key_copy?" + defaultParameters + "' using CassandraStorage();");
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
        Assert.assertEquals(4, count);
    }

    private String getColumnValue(String ks, String cf, String colName, String key, String validator) throws TException, IOException
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
}
