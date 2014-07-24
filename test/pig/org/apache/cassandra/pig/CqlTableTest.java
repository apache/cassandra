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
import java.nio.charset.CharacterCodingException;
import java.util.Iterator;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.thrift.AuthenticationException;
import org.apache.cassandra.thrift.AuthorizationException;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class CqlTableTest extends PigTestBase
{    
    private static String[] statements = {
            "CREATE KEYSPACE cql3ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}",
            "USE cql3ks;",

            "CREATE TABLE cqltable (key1 text, key2 int, column1 int, column2 float, primary key(key1, key2))",
            "INSERT INTO cqltable (key1, key2, column1, column2) values ('key1', 111, 100, 10.1)",
            "CREATE TABLE compactcqltable (key1 text, column1 int, column2 float, primary key(key1)) WITH COMPACT STORAGE",
            "INSERT INTO compactcqltable (key1, column1, column2) values ('key1', 100, 10.1)",

            "CREATE TABLE test (a int PRIMARY KEY, b int);",
            "CREATE INDEX test_b on test (b);",

            "CREATE TABLE moredata (x int PRIMARY KEY, y int);",
            "INSERT INTO test (a,b) VALUES (1,1);",
            "INSERT INTO test (a,b) VALUES (2,2);",
            "INSERT INTO test (a,b) VALUES (3,3);",
            "INSERT INTO moredata (x, y) VALUES (4,4);",
            "INSERT INTO moredata (x, y) VALUES (5,5);",
            "INSERT INTO moredata (x, y) VALUES (6,6);",

            "CREATE TABLE compotable (a int, b int, c text, d text, PRIMARY KEY (a,b,c));",
            "INSERT INTO compotable (a, b , c , d ) VALUES ( 1,1,'One','match');",
            "INSERT INTO compotable (a, b , c , d ) VALUES ( 2,2,'Two','match');",
            "INSERT INTO compotable (a, b , c , d ) VALUES ( 3,3,'Three','match');",
            "INSERT INTO compotable (a, b , c , d ) VALUES ( 4,4,'Four','match');",

            "create table compmore (id int PRIMARY KEY, x int, y int, z text, data text);",
            "INSERT INTO compmore (id, x, y, z,data) VALUES (1,5,6,'Fix','nomatch');",
            "INSERT INTO compmore (id, x, y, z,data) VALUES (2,6,5,'Sive','nomatch');",
            "INSERT INTO compmore (id, x, y, z,data) VALUES (3,7,7,'Seven','match');",
            "INSERT INTO compmore (id, x, y, z,data) VALUES (4,8,8,'Eight','match');",
            "INSERT INTO compmore (id, x, y, z,data) VALUES (5,9,10,'Ninen','nomatch');",

            "CREATE TABLE collectiontable(m text PRIMARY KEY, n map<text, text>);",
            "UPDATE collectiontable SET n['key1'] = 'value1' WHERE m = 'book1';",
            "UPDATE collectiontable SET n['key2'] = 'value2' WHERE m = 'book2';",
            "UPDATE collectiontable SET n['key3'] = 'value3' WHERE m = 'book3';",
            "UPDATE collectiontable SET n['key4'] = 'value4' WHERE m = 'book4';",
    };

    @BeforeClass
    public static void setup() throws TTransportException, IOException, InterruptedException, ConfigurationException,
                                      AuthenticationException, AuthorizationException, InvalidRequestException, UnavailableException, TimedOutException, TException, NotFoundException, CharacterCodingException, ClassNotFoundException, NoSuchFieldException, IllegalAccessException, InstantiationException
    {
        startCassandra();
        setupDataByCql(statements);
        startHadoopCluster();
    }

    @Test
    public void testCqlStorageSchema()
    throws AuthenticationException, AuthorizationException, InvalidRequestException, UnavailableException, TimedOutException, TException, NotFoundException, SchemaDisagreementException, IOException
    {
        cqlTableSchemaTest("rows = LOAD 'cql://cql3ks/cqltable?" + defaultParameters + "' USING CqlStorage();");
        compactCqlTableSchemaTest("rows = LOAD 'cql://cql3ks/compactcqltable?" + defaultParameters + "' USING CqlStorage();");
    }

    @Test
    public void testCqlNativeStorageSchema()
    throws AuthenticationException, AuthorizationException, InvalidRequestException, UnavailableException, TimedOutException, TException, NotFoundException, SchemaDisagreementException, IOException
    {
        //input_cql=select * from cqltable where token(key1) > ? and token(key1) <= ?
        cqlTableSchemaTest("rows = LOAD 'cql://cql3ks/cqltable?" + defaultParameters + nativeParameters +  "&input_cql=select%20*%20from%20cqltable%20where%20token(key1)%20%3E%20%3F%20and%20token(key1)%20%3C%3D%20%3F' USING CqlNativeStorage();");

        //input_cql=select * from compactcqltable where token(key1) > ? and token(key1) <= ?
        compactCqlTableSchemaTest("rows = LOAD 'cql://cql3ks/compactcqltable?" + defaultParameters + nativeParameters + "&input_cql=select%20*%20from%20compactcqltable%20where%20token(key1)%20%3E%20%3F%20and%20token(key1)%20%3C%3D%20%3F' USING CqlNativeStorage();");
    }

    private void compactCqlTableSchemaTest(String initialQuery) throws IOException
    {
        pig.registerQuery(initialQuery);
        Iterator<Tuple>  it = pig.openIterator("rows");
        if (it.hasNext()) {
            Tuple t = it.next();
            Assert.assertEquals(t.get(0).toString(), "key1");
            Assert.assertEquals(t.get(1), 100);
            Assert.assertEquals(t.get(2), 10.1f);
            Assert.assertEquals(3, t.size());
        }
        else
        {
            Assert.fail("Failed to get data for query " + initialQuery);
        }
    }

    private void cqlTableSchemaTest(String initialQuery) throws IOException
    {
        pig.registerQuery(initialQuery);
        Iterator<Tuple> it = pig.openIterator("rows");
        if (it.hasNext()) {
            Tuple t = it.next();
            Assert.assertEquals(t.get(0).toString(), "key1");
            Assert.assertEquals(t.get(1), 111);
            Assert.assertEquals(t.get(2), 100);
            Assert.assertEquals(t.get(3), 10.1f);
            Assert.assertEquals(4, t.size());
        }
        else
        {
            Assert.fail("Failed to get data for query " + initialQuery);
        }
    }

    @Test
    public void testCqlStorageSingleKeyTable()
    throws AuthenticationException, AuthorizationException, InvalidRequestException, UnavailableException, TimedOutException, TException, NotFoundException, SchemaDisagreementException, IOException
    {
        SingleKeyTableTest("moretestvalues= LOAD 'cql://cql3ks/moredata?" + defaultParameters + "' USING CqlStorage();");

    }

    @Test
    public void testCqlNativeStorageSingleKeyTable()
    throws AuthenticationException, AuthorizationException, InvalidRequestException, UnavailableException, TimedOutException, TException, NotFoundException, SchemaDisagreementException, IOException
    {
        //input_cql=select * from moredata where token(x) > ? and token(x) <= ?
        SingleKeyTableTest("moretestvalues= LOAD 'cql://cql3ks/moredata?" + defaultParameters + nativeParameters + "&input_cql=select%20*%20from%20moredata%20where%20token(x)%20%3E%20%3F%20and%20token(x)%20%3C%3D%20%3F' USING CqlNativeStorage();");
    }

    private void SingleKeyTableTest(String initialQuery)
    throws AuthenticationException, AuthorizationException, InvalidRequestException, UnavailableException, TimedOutException, TException, NotFoundException, SchemaDisagreementException, IOException
    {
        pig.setBatchOn();
        pig.registerQuery(initialQuery);
        pig.registerQuery("insertformat= FOREACH moretestvalues GENERATE TOTUPLE(TOTUPLE('a',x)),TOTUPLE(y);");
        pig.registerQuery("STORE insertformat INTO 'cql://cql3ks/test?" + defaultParameters + "&output_query=UPDATE+cql3ks.test+set+b+%3D+%3F' USING CqlStorage();");
        pig.executeBatch();
        //(5,5)
        //(6,6)
        //(4,4)
        //(2,2)
        //(3,3)
        //(1,1)
        pig.registerQuery("result= LOAD 'cql://cql3ks/test?" + defaultParameters + "' USING CqlStorage();");
        Iterator<Tuple> it = pig.openIterator("result");
        while (it.hasNext()) {
            Tuple t = it.next();
            Assert.assertEquals(t.get(0), t.get(1));
        }
    }

    @Test
    public void testCqlStorageCompositeKeyTable()
    throws AuthenticationException, AuthorizationException, InvalidRequestException, UnavailableException, TimedOutException, TException, NotFoundException, SchemaDisagreementException, IOException
    {
        CompositeKeyTableTest("moredata= LOAD 'cql://cql3ks/compmore?" + defaultParameters + "' USING CqlStorage();");
    }

    @Test
    public void testCqlNativeStorageCompositeKeyTable()
    throws AuthenticationException, AuthorizationException, InvalidRequestException, UnavailableException, TimedOutException, TException, NotFoundException, SchemaDisagreementException, IOException
    {
        //input_cql=select * from compmore where token(id) > ? and token(id) <= ?
        CompositeKeyTableTest("moredata= LOAD 'cql://cql3ks/compmore?" + defaultParameters + nativeParameters + "&input_cql=select%20*%20from%20compmore%20where%20token(id)%20%3E%20%3F%20and%20token(id)%20%3C%3D%20%3F' USING CqlNativeStorage();");
    }

    private void CompositeKeyTableTest(String initialQuery)
    throws AuthenticationException, AuthorizationException, InvalidRequestException, UnavailableException, TimedOutException, TException, NotFoundException, SchemaDisagreementException, IOException
    {
        pig.setBatchOn();
        pig.registerQuery(initialQuery);
        pig.registerQuery("insertformat = FOREACH moredata GENERATE TOTUPLE (TOTUPLE('a',x),TOTUPLE('b',y), TOTUPLE('c',z)),TOTUPLE(data);");
        pig.registerQuery("STORE insertformat INTO 'cql://cql3ks/compotable?" + defaultParameters + "&output_query=UPDATE%20cql3ks.compotable%20SET%20d%20%3D%20%3F' USING CqlStorage();");
        pig.executeBatch();

        //(5,6,Fix,nomatch)
        //(3,3,Three,match)
        //(1,1,One,match)
        //(2,2,Two,match)
        //(7,7,Seven,match)
        //(8,8,Eight,match)
        //(6,5,Sive,nomatch)
        //(4,4,Four,match)
        //(9,10,Ninen,nomatch)
        pig.registerQuery("result= LOAD 'cql://cql3ks/compotable?" + defaultParameters + "' USING CqlStorage();");
        Iterator<Tuple> it = pig.openIterator("result");
        int count = 0;
        while (it.hasNext()) {
            it.next();
            count ++;
        }
        Assert.assertEquals(count, 9);
    }

    @Test
    public void testCqlStorageCollectionColumnTable()
    throws AuthenticationException, AuthorizationException, InvalidRequestException, UnavailableException, TimedOutException, TException, NotFoundException, SchemaDisagreementException, IOException
    {
        CollectionColumnTableTest("collectiontable= LOAD 'cql://cql3ks/collectiontable?" + defaultParameters + "' USING CqlStorage();");
    }

    @Test
    public void testCqlNativeStorageCollectionColumnTable()
    throws AuthenticationException, AuthorizationException, InvalidRequestException, UnavailableException, TimedOutException, TException, NotFoundException, SchemaDisagreementException, IOException
    {
        //input_cql=select * from collectiontable where token(m) < ? and token(m) <= ?
        CollectionColumnTableTest("collectiontable= LOAD 'cql://cql3ks/collectiontable?" + defaultParameters + nativeParameters + "&input_cql=input_cql%3Dselect%20*%20from%20collectiontable%20where%20token(m)%20%3C%20%3F%20and%20token(m)%20%3C%3D%20%3F' USING CqlNativeStorage();");
    }

    private void CollectionColumnTableTest(String initialQuery)
    throws AuthenticationException, AuthorizationException, InvalidRequestException, UnavailableException, TimedOutException, TException, NotFoundException, SchemaDisagreementException, IOException
    {
        pig.setBatchOn();
        pig.registerQuery(initialQuery);
        pig.registerQuery("recs= FOREACH collectiontable GENERATE TOTUPLE(TOTUPLE('m', m) ), TOTUPLE(TOTUPLE('map', TOTUPLE('m', 'mm'), TOTUPLE('n', 'nn')));");
        pig.registerQuery("STORE recs INTO 'cql://cql3ks/collectiontable?" + defaultParameters + "&output_query=update+cql3ks.collectiontable+set+n+%3D+%3F' USING CqlStorage();");
        pig.executeBatch();

        //(book2,((m,mm),(n,nn)))
        //(book3,((m,mm),(n,nn)))
        //(book4,((m,mm),(n,nn)))
        //(book1,((m,mm),(n,nn)))
        pig.registerQuery("result= LOAD 'cql://cql3ks/collectiontable?" + defaultParameters + "' USING CqlStorage();");
        Iterator<Tuple> it = pig.openIterator("result");
        if (it.hasNext()) {
            Tuple t = it.next();
            Tuple t1 = (Tuple) t.get(1);
            Assert.assertEquals(t1.size(), 2);
            Tuple element1 = (Tuple) t1.get(0);
            Tuple element2 = (Tuple) t1.get(1);
            Assert.assertEquals(element1.get(0), "m");
            Assert.assertEquals(element1.get(1), "mm");
            Assert.assertEquals(element2.get(0), "n");
            Assert.assertEquals(element2.get(1), "nn");
        }
    }

    @Test
    public void testCassandraStorageSchema() throws IOException, ClassNotFoundException, TException, TimedOutException, NotFoundException, InvalidRequestException, NoSuchFieldException, UnavailableException, IllegalAccessException, InstantiationException
    {
        //results: (key1,{((111,),),((111,column1),100),((111,column2),10.1)})
        pig.registerQuery("rows = LOAD 'cassandra://cql3ks/cqltable?" + defaultParameters + "' USING CassandraStorage();");

        //schema: {key: chararray,columns: {(name: (),value: bytearray)}}
        Iterator<Tuple> it = pig.openIterator("rows");
        if (it.hasNext()) {
            Tuple t = it.next();
            String rowKey =  t.get(0).toString();
            Assert.assertEquals(rowKey, "key1");
            DataBag columns = (DataBag) t.get(1);
            Iterator<Tuple> iter = columns.iterator();
            int i = 0;
            while(iter.hasNext())
            {
                i++;
                Tuple column = (Tuple) iter.next();
                if (i==1)
                {
                    Assert.assertEquals(((Tuple) column.get(0)).get(0), 111);
                    Assert.assertEquals(((Tuple) column.get(0)).get(1), "");
                    Assert.assertEquals(column.get(1).toString(), "");
                }
                if (i==2)
                {
                    Assert.assertEquals(((Tuple) column.get(0)).get(0), 111);
                    Assert.assertEquals(((Tuple) column.get(0)).get(1), "column1");
                    Assert.assertEquals(column.get(1), 100);
                }
                if (i==3)
                {
                    Assert.assertEquals(((Tuple) column.get(0)).get(0), 111);
                    Assert.assertEquals(((Tuple) column.get(0)).get(1), "column2");
                    Assert.assertEquals(column.get(1), 10.1f);
                }
            }
            Assert.assertEquals(3, columns.size());
        }

        //results: (key1,(column1,100),(column2,10.1))
        pig.registerQuery("compact_rows = LOAD 'cassandra://cql3ks/compactcqltable?" + defaultParameters + "' USING CassandraStorage();");

        //schema: {key: chararray,column1: (name: chararray,value: int),column2: (name: chararray,value: float)}
        it = pig.openIterator("compact_rows");
        if (it.hasNext()) {
            Tuple t = it.next();
            String rowKey =  t.get(0).toString();
            Assert.assertEquals(rowKey, "key1");
            Tuple column = (Tuple) t.get(1);
            Assert.assertEquals(column.get(0), "column1");
            Assert.assertEquals(column.get(1), 100);
            column = (Tuple) t.get(2);
            Assert.assertEquals(column.get(0), "column2");
            Assert.assertEquals(column.get(1), 10.1f);
        }
    }
}
