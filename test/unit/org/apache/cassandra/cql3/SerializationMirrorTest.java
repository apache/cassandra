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
package org.apache.cassandra.cql3;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Test;

import org.junit.Assert;

public class SerializationMirrorTest extends CQLTester
{

    @Test
    public void testManyClusterings() throws Throwable
    {
        StringBuilder table = new StringBuilder("CREATE TABLE %s (a TEXT");
        StringBuilder cols = new StringBuilder();
        StringBuilder args = new StringBuilder("?");
        List<Object> vals = new ArrayList<>();
        vals.add("a");
        for (int i = 0 ; i < 40 ; i++)
        {
            table.append(", c").append(i).append(" text");
            cols.append(", c").append(i);
            if (ThreadLocalRandom.current().nextBoolean())
                vals.add(Integer.toString(i));
            else
                vals.add("");
            args.append(",?");
        }
        args.append(",?");
        vals.add("value");
        table.append(", v text, PRIMARY KEY ((a)").append(cols).append("))");
        createTable(table.toString());

        execute("INSERT INTO %s (a" + cols + ", v) VALUES (" + args+ ")", vals.toArray());
        flush();
        UntypedResultSet.Row row = execute("SELECT * FROM %s").one();
        for (int i = 0 ; i < row.getColumns().size() ; i++)
            Assert.assertEquals(vals.get(i), row.getString(i == 0 ? "a" : i < 41 ? "c" + (i - 1) : "v"));
    }

}
