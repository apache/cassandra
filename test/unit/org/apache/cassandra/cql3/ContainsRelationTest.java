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
package org.apache.cassandra.cql3;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class ContainsRelationTest extends CQLTester
{
    @Test
    public void testSetContains() throws Throwable 
    {
        createTable("CREATE TABLE %s (account text, id int, categories set<text>, PRIMARY KEY (account, id));");
        execute("CREATE INDEX cat_index_set ON %s(categories);");
        execute("INSERT INTO %s (account, id , categories) VALUES ('test', 5, {'lmn'});");
        
        assertEmpty(execute("SELECT * FROM %s WHERE account = 'xyz' AND categories CONTAINS 'lmn';"));
        
        assertRows(execute("SELECT * FROM %s WHERE categories CONTAINS 'lmn';"), row("test", 5, newHashSet("lmn")));
        assertRows(execute("SELECT * FROM %s WHERE account = 'test' AND categories CONTAINS 'lmn';"), row("test", 5, newHashSet("lmn")));
    }
    
    @Test
    public void testListContains() throws Throwable 
    {
        createTable("CREATE TABLE %s (account text, id int, categories list<text>, PRIMARY KEY (account, id));");
        execute("CREATE INDEX cat_index_list ON %s(categories);");
        execute("INSERT INTO %s (account, id , categories) VALUES ('test', 5, ['lmn']);");
        
        assertEmpty(execute("SELECT * FROM %s WHERE account = 'xyz' AND categories CONTAINS 'lmn';"));
        
        assertRows(execute("SELECT * FROM %s WHERE account = 'test' AND categories CONTAINS 'lmn';"), row("test", 5, newArrayList("lmn")));
        assertRows(execute("SELECT * FROM %s WHERE categories CONTAINS 'lmn';"), row("test", 5, newArrayList("lmn")));
    }
    
    @Test
    public void testMapKeyContains() throws Throwable 
    {
        createTable("CREATE TABLE %s (account text, id int, categories map<text,text>, PRIMARY KEY (account, id));");
        execute("CREATE INDEX cat_index_map_key ON  %s(keys(categories));");
        execute("INSERT INTO %s (account, id , categories) VALUES ('test', 5, {'lmn':'foo'});");
        
        assertEmpty(execute("SELECT * FROM %s WHERE account = 'xyz' AND categories CONTAINS KEY 'lmn';"));
        
        assertRows(execute("SELECT * FROM %s WHERE account = 'test' AND categories CONTAINS KEY 'lmn';"), row("test", 5, ImmutableMap.of("lmn", "foo")));
        assertRows(execute("SELECT * FROM %s WHERE categories CONTAINS KEY 'lmn';"), row("test", 5, ImmutableMap.of("lmn", "foo")));
    }
    
    @Test
    public void testMapValueContains() throws Throwable 
    {
        createTable("CREATE TABLE %s (account text, id int, categories map<text,text>, PRIMARY KEY (account, id));");
        execute("CREATE INDEX cat_index_map_value ON  %s(categories);");
        execute("INSERT INTO %s (account, id , categories) VALUES ('test', 5, {'lmn':'foo'});");
        
        assertEmpty(execute("SELECT * FROM %s WHERE account = 'xyz' AND categories CONTAINS 'foo';"));
        
        assertRows(execute("SELECT * FROM %s WHERE account = 'test' AND categories CONTAINS 'foo';"), row("test", 5, ImmutableMap.of("lmn", "foo")));
        assertRows(execute("SELECT * FROM %s WHERE categories CONTAINS 'foo';"), row("test", 5, ImmutableMap.of("lmn", "foo")));
    }
}
