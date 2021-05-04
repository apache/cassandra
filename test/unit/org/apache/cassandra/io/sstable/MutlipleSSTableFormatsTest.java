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

package org.apache.cassandra.io.sstable;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.format.trieindex.TrieIndexFormat;
import org.assertj.core.api.Assertions;


public class MutlipleSSTableFormatsTest extends CQLTester
{
    private final static Logger logger = LoggerFactory.getLogger(MutlipleSSTableFormatsTest.class);
    private final static int cnt = 100;
    private final static int overlap = 70;
    private final static int deletionCount = 30;

    private final long seed = System.nanoTime();
    private Random random;

    private String savedProp;

    @Before
    public void before() {
        savedProp = System.getProperty(SSTableFormat.FORMAT_DEFAULT_PROP);
        random = new Random(seed);
        logger.info("Using random seed = {}", seed);
    }

    @After
    public void after() {
        if (savedProp == null)
            System.getProperties().remove(SSTableFormat.FORMAT_DEFAULT_PROP);
        else
            System.setProperty(SSTableFormat.FORMAT_DEFAULT_PROP, savedProp);
    }

    private Map<Integer, Integer> createSSTables() throws Throwable {
        Map<Integer, Integer> content = Maps.newHashMap();

        createTable("CREATE TABLE %s (id INT, val INT, PRIMARY KEY (id))");
        disableCompaction();

        int offset = 0;
        for (SSTableFormat.Type formatType : SSTableFormat.Type.values())
        {
            System.setProperty(SSTableFormat.FORMAT_DEFAULT_PROP, formatType.name);

            for (int i = 0; i < cnt; i++)
            {
                int v = random.nextInt();
                content.put(i + offset, v);
                execute("INSERT INTO %s (id, val) VALUES (?, ?)", i + offset, v);
            }
            offset += cnt - overlap;

            flush();
        }

        for (SSTableFormat.Type formatType : SSTableFormat.Type.values())
        {
            System.setProperty(SSTableFormat.FORMAT_DEFAULT_PROP, formatType.name);

            for (int i = 0; i < deletionCount; i++)
            {
                int key = random.nextInt(offset + overlap);
                content.remove(key);
                execute("DELETE FROM %s WHERE id = ?", key);
            }

            flush();
        }

        List<SSTableFormat.Type> createdFormats = createdFormats();
        Assertions.assertThat(createdFormats).hasSameElementsAs(Sets.newHashSet(SSTableFormat.Type.values()));

        return content;
    }

    private void checkRead(Map<Integer, Integer> content) throws Throwable {
        for (Map.Entry<Integer, Integer> entry : content.entrySet())
        {
            UntypedResultSet r = execute("SELECT val FROM %s WHERE id = ?", entry.getKey());
            Assertions.assertThat(r.one().getInt("val")).isEqualTo(entry.getValue());
        }

        Iterator<UntypedResultSet.Row> it = execute("SELECT id, val FROM %s").iterator();
        Map<Integer, Integer> results = Maps.newHashMap();
        while (it.hasNext()) {
            UntypedResultSet.Row row = it.next();
            results.put(row.getInt("id"), row.getInt("val"));
        }
        Assertions.assertThat(results).isEqualTo(content);
    }

    @Test
    public void testRead() throws Throwable
    {
        Map<Integer, Integer> content = createSSTables();
        checkRead(content);
    }

    @Test
    public void testCompactionToBigFormat() throws Throwable
    {
        testCompaction(BigFormat.instance);
    }

    @Test
    public void testCompactionToBtiFormat() throws Throwable
    {
        testCompaction(TrieIndexFormat.instance);
    }

    private void testCompaction(SSTableFormat format) throws Throwable
    {
        Map<Integer, Integer> content = createSSTables();
        System.setProperty(SSTableFormat.FORMAT_DEFAULT_PROP, format.getType().name);
        enableCompaction();
        compact();
        List<SSTableFormat.Type> createdFormats = createdFormats();
        Assertions.assertThat(createdFormats).hasSize(1);
        Assertions.assertThat(createdFormats.get(0)).isEqualTo(format.getType());
        checkRead(content);
    }

    private List<SSTableFormat.Type> createdFormats()
    {
        return ColumnFamilyStore.getIfExists(KEYSPACE, currentTable())
                                .getLiveSSTables()
                                .stream()
                                .map(sstr -> sstr.descriptor.formatType)
                                .collect(Collectors.toList());
    }

}
