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
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.analyzer.filter.BuiltInAnalyzers;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

public class LuceneAnalyzerTest extends SAITester
{
    @Test
    public void testQueryAnalyzer() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        assertThatThrownBy(() -> createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {" +
                                             "'index_analyzer': '{\n" +
                                             "\t\"tokenizer\":{\"name\":\"ngram\", \"args\":{\"minGramSize\":\"2\", \"maxGramSize\":\"3\"}}," +
                                             "\t\"filters\":[{\"name\":\"lowercase\"}]\n" +
                                             "}'," +
                                             "'query_analyzer': '{\n" +
                                             "\t\"tokenizer\":{\"name\":\"whitespace\"},\n" +
                                             "\t\"filters\":[{\"name\":\"porterstem\"}]\n" +
                                             "}'};"))
        .hasCauseInstanceOf(ConfigurationException.class)
        .hasRootCauseMessage("Properties specified [query_analyzer] are not understood by StorageAttachedIndex");
    }

    @Test
    public void testStandardAnalyzerWithFullConfig() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        createIndex("CREATE CUSTOM INDEX ON %s(val) " +
                    "USING 'org.apache.cassandra.index.sai.StorageAttachedIndex' " +
                    "WITH OPTIONS = { 'index_analyzer': '{" +
                    "    \"tokenizer\" : {\"name\" : \"standard\"}," +
                    "    \"filters\" : [ {\"name\" : \"lowercase\"}] \n" +
                    "  }'}");
        standardAnalyzerTest();
    }

    @Test
    public void testStandardAnalyzerWithBuiltInName() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        createIndex("CREATE CUSTOM INDEX ON %s(val) " +
                    "USING 'org.apache.cassandra.index.sai.StorageAttachedIndex' " +
                    "WITH OPTIONS = { 'index_analyzer': 'standard'}");
        standardAnalyzerTest();
    }

    private void standardAnalyzerTest() throws Throwable {
        waitForIndexQueryable();
        execute("INSERT INTO %s (id, val) VALUES ('1', 'The quick brown fox jumps over the lazy DOG.')");

        assertEquals(1, execute("SELECT * FROM %s WHERE val = 'The quick brown fox jumps over the lazy DOG.' ALLOW FILTERING").size());

        flush();
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'dog'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'dog' OR val : 'missing'").size());
        assertEquals(0, execute("SELECT * FROM %s WHERE val : 'missing1' OR val : 'missing2'").size());
        assertEquals(0, execute("SELECT * FROM %s WHERE val : 'dog' AND val : 'missing'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'dog' AND val : 'lazy'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'dog' AND val : 'quick' AND val : 'fox'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'dog' AND val : 'quick' OR val : 'missing'").size());

        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'dog' AND (val : 'quick' OR val : 'missing')").size());
        assertEquals(0, execute("SELECT * FROM %s WHERE val : 'missing' AND (val : 'quick' OR val : 'dog')").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'dog' OR (val : 'quick' AND val : 'missing')").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'missing' OR (val : 'quick' AND val : 'dog')").size());
        assertEquals(0, execute("SELECT * FROM %s WHERE val : 'missing' OR (val : 'quick' AND val : 'missing')").size());

        // EQ operator is not supported for analyzed columns unless ALLOW FILTERING is used
        assertThatThrownBy(() -> execute("SELECT * FROM %s WHERE val = 'dog'")).isInstanceOf(InvalidRequestException.class);
        assertEquals(1, execute("SELECT * FROM %s WHERE val = 'The quick brown fox jumps over the lazy DOG.' ALLOW FILTERING").size());
        // EQ is a raw equality check, so a token like 'dog' should not return any results
        assertEquals(0, execute("SELECT * FROM %s WHERE val = 'dog' ALLOW FILTERING").size());
    }

    @Test
    public void testEmptyAnalyzerFailsAtCreation() {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        assertThatThrownBy(() -> createIndex("CREATE CUSTOM INDEX ON %s(val) " +
                                             "USING 'org.apache.cassandra.index.sai.StorageAttachedIndex' " +
                                             "WITH OPTIONS = { 'index_analyzer': '{}'}"))
        .isInstanceOf(InvalidRequestException.class)
        .hasRootCauseMessage("Analzyer config requires at least a tokenizer, a filter, or a charFilter, but none found. config={}");
    }

    @Test
    public void testIndexAnalyzerAndNonTokenizingAnalyzerFailsAtCreation() {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        assertThatThrownBy(() -> createIndex("CREATE CUSTOM INDEX ON %s(val) " +
                                             "USING 'org.apache.cassandra.index.sai.StorageAttachedIndex' " +
                                             "WITH OPTIONS = { 'index_analyzer': 'standard', 'ascii': true}"))
        .isInstanceOf(InvalidRequestException.class)
        .hasRootCauseMessage("Cannot specify case_insensitive, normalize, or ascii options with " +
                             "index_analyzer option. options={index_analyzer=standard, ascii=true, target=val}");

        assertThatThrownBy(() -> createIndex("CREATE CUSTOM INDEX ON %s(val) " +
                                             "USING 'org.apache.cassandra.index.sai.StorageAttachedIndex' " +
                                             "WITH OPTIONS = { 'index_analyzer': 'standard', 'normalize': true}"))
        .isInstanceOf(InvalidRequestException.class)
        .hasRootCauseMessage("Cannot specify case_insensitive, normalize, or ascii options with " +
                             "index_analyzer option. options={index_analyzer=standard, normalize=true, target=val}");

        assertThatThrownBy(() -> createIndex("CREATE CUSTOM INDEX ON %s(val) " +
                                             "USING 'org.apache.cassandra.index.sai.StorageAttachedIndex' " +
                                             "WITH OPTIONS = { 'index_analyzer': 'standard', 'case_sensitive': false}"))
        .isInstanceOf(InvalidRequestException.class)
        .hasRootCauseMessage("Cannot specify case_insensitive, normalize, or ascii options with " +
                             "index_analyzer option. options={index_analyzer=standard, case_sensitive=false, target=val}");
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

        assertThatThrownBy(() -> execute("SELECT * FROM %s WHERE val : 'dog'"))
        .isInstanceOf(InvalidRequestException.class);;

        // Equality still works because indexed value is not analyzed, and so the search can be performed without
        // filtering.
        assertEquals(1, execute("SELECT * FROM %s WHERE val = 'dog'").size());
    }

    // Analyzers on clustering columns are not supported yet
    @Test
    public void testStandardAnalyzerInClusteringColumnFailsAtCreateIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (id int, val text, PRIMARY KEY (id, val))");

        assertThatThrownBy(() -> createIndex("CREATE CUSTOM INDEX ON %s(val) " +
                                             "USING 'org.apache.cassandra.index.sai.StorageAttachedIndex' " +
                                             "WITH OPTIONS = { 'index_analyzer': 'standard' }"
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

        assertThatThrownBy(
        () -> executeNet("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer':'lalalalaal'}"
        )).isInstanceOf(InvalidQueryException.class);

        assertThatThrownBy(
        () -> executeNet("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = " +
                         "{'index_analyzer':'{\"tokenizer\" : {\"name\" : \"lalala\"}}'}"
        )).isInstanceOf(InvalidQueryException.class);
    }

    @Test
    public void testStopFilter() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        assertThatThrownBy(() -> executeNet("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer':'\n" +
                                            "\t{\"tokenizer\":{\"name\" : \"whitespace\"},\n" +
                                            "\t \"filters\":[{\"name\":\"stop\"}]}'}"))
        .isInstanceOf(InvalidQueryException.class)
        .hasMessageContaining("filter=stop is unsupported.");
    }

    @Test
    public void testCharfilter() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer':'{\n" +
                    "\t\"tokenizer\":{\"name\":\"keyword\"},\n" +
                    "\t\"charFilters\":[{\"name\":\"htmlstrip\"}]\n" +
                    "}'}");

        waitForIndexQueryable();

        execute("INSERT INTO %s (id, val) VALUES ('1', '<b>hello</b>')");

        flush();

        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'hello'").size());
    }

    @Test
    public void testNGramfilter() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        String ddl = "CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer':'\n" +
                     "\t{\"tokenizer\":{\"name\":\"ngram\", \"args\":{\"minGramSize\":\"2\", \"maxGramSize\":\"3\"}}," +
                     "\t\"filters\":[{\"name\":\"lowercase\"}]}'}";
        createIndex(ddl);

        waitForIndexQueryable();

        execute("INSERT INTO %s (id, val) VALUES ('1', 'DoG')");

        flush();

        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'do'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'og'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'dog'").size());
    }

    @Test
    public void testNGramfilterNoFlush() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer':'\n" +
                    "\t{\"tokenizer\":{\"name\":\"ngram\", \"args\":{\"minGramSize\":\"2\", \"maxGramSize\":\"3\"}}," +
                    "\t\"filters\":[{\"name\":\"lowercase\"}]}'}");

        waitForIndexQueryable();

        execute("INSERT INTO %s (id, val) VALUES ('1', 'DoG')");

        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'do'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'og'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'dog'").size());
    }

    @Test
    public void testWhitespace() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS =" +
                    "{'index_analyzer':'whitespace'}");

        waitForIndexQueryable();

        execute("INSERT INTO %s (id, val) VALUES ('1', 'hello world twice the and')");

        flush();

        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'hello'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'twice'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'the'").size()); // test stop word
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'and'").size()); // test stop word
    }

    @Test
    public void testWhitespaceLowercase() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer':'\n" +
                    "\t{\"tokenizer\":{\"name\":\"whitespace\"}," +
                    "\t\"filters\":[{\"name\":\"lowercase\"}]}'}");

        waitForIndexQueryable();

        execute("INSERT INTO %s (id, val) VALUES ('1', 'hELlo woRlD tWice tHe aNd')");

        flush();

        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'hello'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'twice'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'the'").size()); // test stop word
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'and'").size()); // test stop word
    }

    @Test
    public void testTokenizer() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer':'\n" +
                    "\t{\"tokenizer\":{\"name\":\"whitespace\"}," +
                    "\t\"filters\":[{\"name\":\"porterstem\"}]}'}");

        waitForIndexQueryable();

        execute("INSERT INTO %s (id, val) VALUES ('1', 'the queries test')");

        flush();

        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'the'").size()); // stop word test
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'query'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'query' OR val : 'missing'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE val : 'queries' AND val : 'the' AND val : 'test'").size());
    }

    @Test
    public void testAnalyzerMatchesAndEqualityFailForConjunction() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer':'\n" +
                    "\t{\"tokenizer\":{\"name\":\"whitespace\"}," +
                    "\t\"filters\":[{\"name\":\"porterstem\"}]}'}");

        waitForIndexQueryable();

        execute("INSERT INTO %s (id, val) VALUES ('1', 'the queries test')");

        assertThatThrownBy(() -> execute("SELECT * FROM %s WHERE val : 'queries' AND val : 'the' AND val = 'the queries test'"))
        .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> execute("SELECT * FROM %s WHERE val : 'queries' AND val : 'the' AND val = 'the queries test' ALLOW FILTERING"))
        .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void testBuiltInAlyzerIndexCreation() throws Throwable
    {
        for (BuiltInAnalyzers builtInAnalyzer : BuiltInAnalyzers.values())
            testBuiltInAlyzerIndexCreationFor(builtInAnalyzer.name());
    }

    private void testBuiltInAlyzerIndexCreationFor(String builtInAnalyzerName) throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = " +
                    "{'index_analyzer':'" + builtInAnalyzerName + "'}");

        waitForIndexQueryable();
    }
}