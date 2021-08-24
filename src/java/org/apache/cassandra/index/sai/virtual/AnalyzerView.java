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

package org.apache.cassandra.index.sai.virtual;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.google.common.base.Charsets;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.virtual.AbstractVirtualTable;
import org.apache.cassandra.db.virtual.SimpleDataSet;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.analyzer.JSONAnalyzerParser;
import org.apache.cassandra.index.sai.analyzer.LuceneAnalyzer;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.lucene.analysis.Analyzer;

public class AnalyzerView extends AbstractVirtualTable
{
    public AnalyzerView(String keyspace)
    {
        super(TableMetadata.builder(keyspace, "analyzer")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .partitioner(new LocalPartitioner(CompositeType.getInstance(UTF8Type.instance, UTF8Type.instance)))
                           .addPartitionKeyColumn("text", UTF8Type.instance)
                           .addPartitionKeyColumn("json_analyzer", UTF8Type.instance)
                           .addRegularColumn("tokens", UTF8Type.instance)
                           .build());
    }

    @Override
    public DataSet data()
    {
        return new SimpleDataSet(metadata());
    }

    @Override
    public DataSet data(DecoratedKey partitionKey)
    {
        LuceneAnalyzer luceneAnalyzer = null;
        String text = null;
        String optionsString = null;
        try
        {
            ByteBuffer[] array = ((CompositeType) metadata().partitionKeyType).split(partitionKey.getKey());
            text = UTF8Type.instance.compose(array[0]);
            optionsString = UTF8Type.instance.compose(array[1]);

            Analyzer analyzer = JSONAnalyzerParser.parse(optionsString);
            luceneAnalyzer = new LuceneAnalyzer(UTF8Type.instance, analyzer, new HashMap<>());

            ByteBuffer toAnalyze = ByteBuffer.wrap(text.getBytes(Charsets.UTF_8));
            luceneAnalyzer.reset(toAnalyze);
            ByteBuffer analyzed = null;

            List<String> list = new ArrayList<>();

            while (luceneAnalyzer.hasNext())
            {
                analyzed = luceneAnalyzer.next();

                list.add(ByteBufferUtil.string(analyzed, Charsets.UTF_8));
            }

            SimpleDataSet result = new SimpleDataSet(metadata());
            result.row(text, optionsString).column("tokens", list.toString());
            return result;
        }
        catch (Exception ex)
        {
            throw new InvalidRequestException("Unable to analyze text="+text+" json_analyzer="+optionsString, ex);
        }
        finally
        {
            if (luceneAnalyzer != null)
            {
                luceneAnalyzer.end();
            }
        }
    }
}
