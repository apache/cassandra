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

import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.thrift.AuthenticationException;
import org.apache.cassandra.thrift.AuthorizationException;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.utils.Hex;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class ThriftColumnFamilyDataTypeTest extends PigTestBase
{
    //AsciiType
    //LongType
    //BytesType
    //BooleanType
    //CounterColumnType
    //DecimalType
    //DoubleType
    //FloatType
    //InetAddressType
    //Int32Type
    //UTF8Type
    //DateType
    //UUIDType
    //IntegerType
    //TimeUUIDType
    //IntegerType
    //LexicalUUIDType
    private static String[] statements = {
            "create keyspace thriftKs with placement_strategy = 'org.apache.cassandra.locator.SimpleStrategy' and" +
            " strategy_options={replication_factor:1};",
            "use thriftKs;",

            "create column family SomeApp " +
                    " with comparator = UTF8Type " +
                    " and default_validation_class = UTF8Type " +
                    " and key_validation_class = UTF8Type " +
                    " and column_metadata = [" +
                    "{column_name: col_ascii, validation_class: AsciiType}, " +
                    "{column_name: col_long, validation_class: LongType}, " +
                    "{column_name: col_bytes, validation_class: BytesType}, " +
                    "{column_name: col_boolean, validation_class: BooleanType}, " +
                    "{column_name: col_decimal, validation_class: DecimalType}, " +
                    "{column_name: col_double, validation_class: DoubleType}, " +
                    "{column_name: col_float, validation_class: FloatType}," +
                    "{column_name: col_inetaddress, validation_class: InetAddressType}, " +
                    "{column_name: col_int32, validation_class: Int32Type}, " +
                    "{column_name: col_uft8, validation_class: UTF8Type}, " +
                    "{column_name: col_date, validation_class: DateType}, " +
                    "{column_name: col_uuid, validation_class: UUIDType}, " +
                    "{column_name: col_integer, validation_class: IntegerType}, " +
                    "{column_name: col_timeuuid, validation_class: TimeUUIDType}, " +
                    "{column_name: col_lexical_uuid, validation_class: LexicalUUIDType}, " +
                    "]; ",

             "set SomeApp['foo']['col_ascii'] = 'ascii';",
             "set SomeApp['foo']['col_boolean'] = false;",
             "set SomeApp['foo']['col_bytes'] = 'DEADBEEF';",
             "set SomeApp['foo']['col_date'] = '2011-02-03T04:05:00+0000';",
             "set SomeApp['foo']['col_decimal'] = '23.345';",
             "set SomeApp['foo']['col_double'] = '2.7182818284590451';",
             "set SomeApp['foo']['col_float'] = '23.45';",
             "set SomeApp['foo']['col_inetaddress'] = '127.0.0.1';",          
             "set SomeApp['foo']['col_int32'] = 23;",
             "set SomeApp['foo']['col_integer'] = 12345;",
             "set SomeApp['foo']['col_long'] = 12345678;",
             "set SomeApp['foo']['col_lexical_uuid'] = 'e23f450f-53a6-11e2-7f7f-7f7f7f7f7f77';",
             "set SomeApp['foo']['col_timeuuid'] = 'e23f450f-53a6-11e2-7f7f-7f7f7f7f7f7f';",
             "set SomeApp['foo']['col_uft8'] = 'hello';",
             "set SomeApp['foo']['col_uuid'] = '550e8400-e29b-41d4-a716-446655440000';",

             "create column family CC with " +
                       "key_validation_class = UTF8Type and " +
                       "default_validation_class=CounterColumnType " +
                       "and comparator=UTF8Type;",

             "incr CC['chuck']['kick'];",
             "incr CC['chuck']['kick'];",
             "incr CC['chuck']['kick'];"
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
    public void testCassandraStorageDataType() throws IOException, ClassNotFoundException, TException, TimedOutException, NotFoundException, InvalidRequestException, NoSuchFieldException, UnavailableException, IllegalAccessException, InstantiationException
    {
        pig.registerQuery("rows = LOAD 'cassandra://thriftKs/SomeApp?" + defaultParameters + "' USING CassandraStorage();");

        //{key: chararray, col_ascii: (name: chararray,value: chararray),
        //col_boolean: (name: chararray,value: bytearray),
        //col_bytes: (name: chararray,value: bytearray),
        //col_date: (name: chararray,value: long),
        //col_decimal: (name: chararray,value: chararray),
        //col_double: (name: chararray,value: double),
        //col_float: (name: chararray,value: float),
        //col_inetaddress: (name: chararray,value: chararray),
        //col_int32: (name: chararray,value: int),
        //col_integer: (name: chararray,value: int),
        //col_lexical_uuid: (name: chararray,value: chararray),
        //col_long: (name: chararray,value: long),
        //col_timeuuid: (name: chararray,value: bytearray),
        //col_uft8: (name: chararray,value: chararray),
        //col_uuid: (name: chararray,value: chararray),
        //columns: {(name: chararray,value: chararray)}}
        Iterator<Tuple> it = pig.openIterator("rows");
        if (it.hasNext()) {
            Tuple t = it.next();
            Assert.assertEquals(t.get(0), "foo");
            Tuple column = (Tuple) t.get(1);
            Assert.assertEquals(column.get(1), "ascii");
            column = (Tuple) t.get(2);
            Assert.assertEquals(column.get(1), false);
            column = (Tuple) t.get(3);
            Assert.assertEquals(column.get(1), new DataByteArray(Hex.hexToBytes("DEADBEEF")));
            column = (Tuple) t.get(4);
            Assert.assertEquals(column.get(1), 1296705900000L);
            column = (Tuple) t.get(5);
            Assert.assertEquals(column.get(1), "23.345");
            column = (Tuple) t.get(6);
            Assert.assertEquals(column.get(1), 2.7182818284590451d);
            column = (Tuple) t.get(7);
            Assert.assertEquals(column.get(1), 23.45f);
            column = (Tuple) t.get(8);
            Assert.assertEquals(column.get(1), "127.0.0.1");
            column = (Tuple) t.get(9);
            Assert.assertEquals(column.get(1), 23);
            column = (Tuple) t.get(10);
            Assert.assertEquals(column.get(1), 12345);
            column = (Tuple) t.get(11);
            Assert.assertEquals(column.get(1), new DataByteArray((TimeUUIDType.instance.fromString("e23f450f-53a6-11e2-7f7f-7f7f7f7f7f77").array())));
            column = (Tuple) t.get(12);
            Assert.assertEquals(column.get(1), 12345678L);
            column = (Tuple) t.get(13);
            Assert.assertEquals(column.get(1), new DataByteArray((TimeUUIDType.instance.fromString("e23f450f-53a6-11e2-7f7f-7f7f7f7f7f7f").array())));
            column = (Tuple) t.get(14);
            Assert.assertEquals(column.get(1), "hello");
            column = (Tuple) t.get(15);
            Assert.assertEquals(column.get(1), new DataByteArray((UUIDType.instance.fromString("550e8400-e29b-41d4-a716-446655440000").array())));
        }

        pig.registerQuery("cc_rows = LOAD 'cassandra://thriftKs/CC?" + defaultParameters + "' USING CassandraStorage();");

        //(chuck,{(kick,3)})
        it = pig.openIterator("cc_rows");
        if (it.hasNext()) {
            Tuple t = it.next();
            Assert.assertEquals(t.get(0), "chuck");           
            DataBag columns = (DataBag) t.get(1);
            Iterator<Tuple> iter = columns.iterator();
            if(iter.hasNext())
            {
                Tuple column = iter.next();
                Assert.assertEquals(column.get(0), "kick");
                Assert.assertEquals(column.get(1), 3L);
            }
         }
    }
}
