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

package org.apache.cassandra.index.sai.cql;

import org.junit.Test;

import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.SAITester;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

public class LuceneAnalyzerTest extends SAITester
{
    @Test
    public void testQueryAnalyzer() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {" +
                    "'index_analyzer': '[\n" +
                    "\t{\"tokenizer\":\"ngram\", \"minGramSize\":\"2\", \"maxGramSize\":\"3\"},\n" +
                    "\t{\"filter\":\"lowercase\"}\n" +
                    "]'," +
                    "'query_analyzer': '[\n" +
                    "\t{\"tokenizer\":\"whitespace\"},\n" +
                    "\t{\"filter\":\"porterstem\"}\n" +
                    "]'};");

        waitForIndexQueryable();

        execute("INSERT INTO %s (id, val) VALUES ('1', 'the query')");

        // TODO: randomize flushing... not sure how
        flush();

        assertEquals(0, execute("SELECT * FROM %s WHERE val = 'query'").size());
    }

    @Test
    public void testStandardAnalyzer() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        createIndex("CREATE CUSTOM INDEX ON %s(val) " +
                    "USING 'org.apache.cassandra.index.sai.StorageAttachedIndex' " +
                    "WITH OPTIONS = { 'index_analyzer': '[{\"tokenizer\": \"standard\"}, {\"filter\": \"lowercase\"}]' }");

        waitForIndexQueryable();

        execute("INSERT INTO %s (id, val) VALUES ('1', 'The quick brown fox jumps over the lazy DOG.')");

        assertEquals(1, execute("SELECT * FROM %s WHERE val = 'dog'").size());

        flush();
        assertEquals(1, execute("SELECT * FROM %s WHERE val = 'dog'").size());
    }

    // Technically, the NoopAnalyzer is applied, but that maps each field without modification, so any operator
    // that matches the SAI field will also match the PK field when compared later in the search (there are two phases).
    @Test
    public void testNoAnalyzerOnClusteredColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (id int, val text, PRIMARY KEY (id, val))");

        createIndex("CREATE CUSTOM INDEX ON %s(val) " +
                    "USING 'org.apache.cassandra.index.sai.StorageAttachedIndex'");

        waitForIndexQueryable();

        execute("INSERT INTO %s (id, val) VALUES (1, 'dog')");

        assertEquals(1, execute("SELECT * FROM %s WHERE val = 'dog'").size());

        flush();
        assertEquals(1, execute("SELECT * FROM %s WHERE val = 'dog'").size());
    }

    // Analyzers on clustering columns are not supported yet
    @Test
    public void testStandardAnalyzerInClusteringColumnFailsAtCreateIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (id int, val text, PRIMARY KEY (id, val))");

        assertThatThrownBy(() -> createIndex("CREATE CUSTOM INDEX ON %s(val) " +
                    "USING 'org.apache.cassandra.index.sai.StorageAttachedIndex' " +
                    "WITH OPTIONS = { 'index_analyzer': '[{\"tokenizer\": \"standard\"}, {\"filter\": \"lowercase\"}]' }"
        )).isInstanceOf(InvalidRequestException.class);

        assertThatThrownBy(() -> createIndex("CREATE CUSTOM INDEX ON %s(val) WITH OPTIONS = { 'ascii': true }"
        )).isInstanceOf(InvalidRequestException.class);

        assertThatThrownBy(() -> createIndex("CREATE CUSTOM INDEX ON %s(val) " +
                                             "WITH OPTIONS = { 'case_sesnsitive': false }"
        )).isInstanceOf(InvalidRequestException.class);

        assertThatThrownBy(() -> createIndex("CREATE CUSTOM INDEX ON %s(val) " +
                                             "WITH OPTIONS = { 'normalize': true }"
        )).isInstanceOf(InvalidRequestException.class);
    }

    @Test
    public void testBogusAnalyzer() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        assertThatThrownBy(() -> executeNet("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer':'[\n" +
                                            "\t{\"tokenizer\":\"lalalalaal\"}\n" +
                                            "]'}")).isInstanceOf(InvalidQueryException.class);
    }

    @Test
    public void testStopFilter() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        assertThatThrownBy(() -> executeNet("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer':'[\n" +
                                            "\t{\"tokenizer\":\"whitespace\"},\n" +
                                            "\t{\"filter\":\"stop\"}\n" +
                                            "]'}")).isInstanceOf(InvalidQueryException.class);
    }

    @Test
    public void testCharfilter() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer':'[\n" +
                    "\t{\"tokenizer\":\"keyword\"},\n" +
                    "\t{\"charfilter\":\"htmlstrip\"}\n" +
                    "]'}");

        waitForIndexQueryable();

        execute("INSERT INTO %s (id, val) VALUES ('1', '<b>hello</b>')");

        flush();

        assertEquals(1, execute("SELECT * FROM %s WHERE val = 'hello'").size());
    }

    @Test
    public void testNGramfilter() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        String ddl = "CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer':'[\n" +
                       "\t{\"tokenizer\":\"ngram\", \"minGramSize\":\"2\", \"maxGramSize\":\"3\"},\n" +
                       "\t{\"filter\":\"lowercase\"}\n" +
                       "]'}";
        createIndex(ddl);

        waitForIndexQueryable();

        execute("INSERT INTO %s (id, val) VALUES ('1', 'DoG')");

        flush();

        assertEquals(1, execute("SELECT * FROM %s WHERE val = 'do'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val = 'og'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val = 'dog'").size());
    }

    @Test
    public void testNGramfilterNoFlush() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer':'[\n" +
                    "\t{\"tokenizer\":\"ngram\", \"minGramSize\":\"2\", \"maxGramSize\":\"3\"},\n" +
                    "\t{\"filter\":\"lowercase\"}\n" +
                    "]'}");

        waitForIndexQueryable();

        execute("INSERT INTO %s (id, val) VALUES ('1', 'DoG')");

        assertEquals(1, execute("SELECT * FROM %s WHERE val = 'do'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val = 'og'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val = 'dog'").size());
    }

    @Test
    public void testWhitespace() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer':'[\n" +
                    "{\"tokenizer\":\"whitespace\"}\n" +
                    "]'}");

        waitForIndexQueryable();

        execute("INSERT INTO %s (id, val) VALUES ('1', 'hello world twice the and')");

        flush();

        assertEquals(1, execute("SELECT * FROM %s WHERE val = 'hello'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val = 'twice'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val = 'the'").size()); // test stop word
        assertEquals(1, execute("SELECT * FROM %s WHERE val = 'and'").size()); // test stop word
    }

    @Test
    public void testWhitespaceLowercase() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer':'[\n" +
                    "\t{\"tokenizer\":\"whitespace\"},\n" +
                    "\t{\"filter\":\"lowercase\"}\n" +
                    "]'}");

        waitForIndexQueryable();

        execute("INSERT INTO %s (id, val) VALUES ('1', 'hELlo woRlD tWice tHe aNd')");

        flush();

        assertEquals(1, execute("SELECT * FROM %s WHERE val = 'hello'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val = 'twice'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val = 'the'").size()); // test stop word
        assertEquals(1, execute("SELECT * FROM %s WHERE val = 'and'").size()); // test stop word
    }

    @Test
    public void testTokenizer() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer':'[\n" +
                    "\t{\"tokenizer\":\"whitespace\"},\n" +
                    "\t{\"filter\":\"porterstem\"}\n" +
                    "]'}");

        waitForIndexQueryable();

        execute("INSERT INTO %s (id, val) VALUES ('1', 'the queries')");

        flush();

        assertEquals(1, execute("SELECT * FROM %s WHERE val = 'the'").size()); // stop word test
        assertEquals(1, execute("SELECT * FROM %s WHERE val = 'query'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val = 'queries'").size());
    }
}
