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

package org.apache.cassandra.index.sai.disk.v1;

import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SSTableQueryContext;
import org.apache.cassandra.index.sai.disk.FileUtils;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.QueryEventListeners;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.bitpack.NumericValuesMeta;
import org.apache.cassandra.index.sai.disk.v1.kdtree.BKDReader;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.trieindex.TrieIndexFormat;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.apache.cassandra.index.sai.disk.v1.kdtree.BKDQueries.bkdQueryFrom;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Note: The sstables and SAI indexes used in this test were written with DSE 6.8
 * in order to guarantee the correctness of the V1 on-disk format code.
 */
public class LegacyOnDiskFormatTest
{
    private TemporaryFolder temporaryFolder = new TemporaryFolder();
    private Descriptor descriptor;
    private TableMetadata tableMetadata;
    private IndexDescriptor indexDescriptor;
    private SSTableReader sstable;

    @BeforeClass
    public static void initialise()
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
        StorageService.instance.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }


    @Before
    public void setup() throws Throwable
    {
        temporaryFolder.create();
        descriptor = Descriptor.fromFilename(temporaryFolder.newFolder().getAbsolutePath() + "/bb-1-bti-Data.db");
        FileUtils.copySSTablesAndIndexes(descriptor, "aa");
        tableMetadata = TableMetadata.builder("test", "test")
                                     .addPartitionKeyColumn("pk", Int32Type.instance)
                                     .addRegularColumn("int_value", Int32Type.instance)
                                     .addRegularColumn("text_value", UTF8Type.instance)
                                     .build();
        sstable = TrieIndexFormat.instance.getReaderFactory().openNoValidation(descriptor, TableMetadataRef.forOfflineTools(tableMetadata));
        indexDescriptor = IndexDescriptor.create(sstable);
    }

    @After
    public void teardown()
    {
        temporaryFolder.delete();
    }

    @Test
    public void canReadPerSSTableMetadata() throws Throwable
    {
        final MetadataSource source = MetadataSource.loadGroupMetadata(indexDescriptor);

        NumericValuesMeta numericValuesMeta = new NumericValuesMeta(source.get(indexDescriptor.componentName(IndexComponent.OFFSETS_VALUES, null)));

        assertEquals(100, numericValuesMeta.valueCount);

        numericValuesMeta = new NumericValuesMeta(source.get(indexDescriptor.componentName(IndexComponent.TOKEN_VALUES, null)));

        assertEquals(100, numericValuesMeta.valueCount);
    }

    @Test
    public void canReadPerIndexMetadata() throws Throwable
    {
        final MetadataSource source = MetadataSource.loadColumnMetadata(indexDescriptor, SAITester.createIndexContext("int_index",
                                                                                                                      Int32Type.instance));

        List<SegmentMetadata> metadatas = SegmentMetadata.load(source, indexDescriptor.primaryKeyFactory);

        assertEquals(1, metadatas.size());
        assertEquals(100, metadatas.get(0).numRows);
    }

    @Test
    public void canCreateAndUsePrimaryKeyMapWithLegacyFormat() throws Throwable
    {
        PrimaryKeyMap.Factory primaryKeyMapFactory = indexDescriptor.newPrimaryKeyMapFactory(sstable);

        PrimaryKeyMap primaryKeyMap = primaryKeyMapFactory.newPerSSTablePrimaryKeyMap(SSTableQueryContext.forTest());

        PrimaryKey expected = indexDescriptor.primaryKeyFactory.createTokenOnly(Murmur3Partitioner.instance.decorateKey(Int32Type.instance.decompose(23)).getToken());

        PrimaryKey primaryKey = primaryKeyMap.primaryKeyFromRowId(0);

        assertEquals(expected, primaryKey);
    }

    @Test
    public void canSearchBDKIndex() throws Throwable
    {
        IndexContext indexContext = SAITester.createIndexContext("int_index", Int32Type.instance);

        final MetadataSource source = MetadataSource.loadColumnMetadata(indexDescriptor, SAITester.createIndexContext("int_index",
                                                                                                                      Int32Type.instance));

        List<SegmentMetadata> metadatas = SegmentMetadata.load(source, indexDescriptor.primaryKeyFactory);

        BKDReader bkdReader = new BKDReader(indexContext,
                                            indexDescriptor.createPerIndexFileHandle(IndexComponent.KD_TREE, indexContext, false),
                                            metadatas.get(0).getIndexRoot(IndexComponent.KD_TREE),
                                            indexDescriptor.createPerIndexFileHandle(IndexComponent.KD_TREE_POSTING_LISTS, indexContext, false),
                                            metadatas.get(0).getIndexRoot(IndexComponent.KD_TREE_POSTING_LISTS));

        Expression expression = new Expression(indexContext).add(Operator.LT, Int32Type.instance.decompose(10));
        BKDReader.IntersectVisitor query = bkdQueryFrom(expression, bkdReader.getNumDimensions(), bkdReader.getBytesPerDimension());
        PostingList postingList = bkdReader.intersect(query, QueryEventListeners.NO_OP_BKD_LISTENER, new QueryContext());
        assertNotNull(postingList);
    }

    @Test
    public void canSearchTermsIndex() throws Throwable
    {
        IndexContext indexContext = SAITester.createIndexContext("text_index", UTF8Type.instance);

        final MetadataSource source = MetadataSource.loadColumnMetadata(indexDescriptor, indexContext);

        SegmentMetadata metadata = SegmentMetadata.load(source, indexDescriptor.primaryKeyFactory).get(0);

        long root = metadata.getIndexRoot(IndexComponent.TERMS_DATA);
        Map<String,String> map = metadata.componentMetadatas.get(IndexComponent.TERMS_DATA).attributes;
        String footerPointerString = map.get(SAICodecUtils.FOOTER_POINTER);
        long footerPointer = footerPointerString == null ? -1 : Long.parseLong(footerPointerString);

        TermsReader termsReader = new TermsReader(indexContext,
                                                  indexDescriptor.createPerIndexFileHandle(IndexComponent.TERMS_DATA, indexContext, false),
                                                  indexDescriptor.createPerIndexFileHandle(IndexComponent.POSTING_LISTS, indexContext, false),
                                                  root,
                                                  footerPointer);
        Expression expression = new Expression(indexContext).add(Operator.EQ, UTF8Type.instance.decompose("10"));
        ByteComparable term = ByteComparable.fixedLength(expression.lower.value.encoded);

        PostingList result = termsReader.exactMatch(term, QueryEventListeners.NO_OP_TRIE_LISTENER, new QueryContext());

        assertEquals(1, result.size());
    }
}
