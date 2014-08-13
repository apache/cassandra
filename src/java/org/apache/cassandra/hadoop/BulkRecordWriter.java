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
package org.apache.cassandra.hadoop;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.io.sstable.SSTableLoader;
import org.apache.cassandra.io.sstable.SSTableSimpleUnsortedWriter;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.CounterColumn;
import org.apache.cassandra.thrift.Mutation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.Progressable;

public final class BulkRecordWriter extends AbstractBulkRecordWriter<ByteBuffer, List<Mutation>>
{
    private File outputDir;
    
    
    private enum CFType
    {
        NORMAL,
        SUPER,
    }

    private enum ColType
    {
        NORMAL,
        COUNTER
    }

    private CFType cfType;
    private ColType colType;

    BulkRecordWriter(TaskAttemptContext context)
    {
        super(context);
    }

    BulkRecordWriter(Configuration conf, Progressable progress)
    {
        super(conf, progress);
    }

    BulkRecordWriter(Configuration conf)
    {
        super(conf);
    }

    private void setTypes(Mutation mutation)
    {
       if (cfType == null)
       {
           if (mutation.getColumn_or_supercolumn().isSetSuper_column() || mutation.getColumn_or_supercolumn().isSetCounter_super_column())
               cfType = CFType.SUPER;
           else
               cfType = CFType.NORMAL;
           if (mutation.getColumn_or_supercolumn().isSetCounter_column() || mutation.getColumn_or_supercolumn().isSetCounter_super_column())
               colType = ColType.COUNTER;
           else
               colType = ColType.NORMAL;
       }
    }

    private void prepareWriter() throws IOException
    {
        if (outputDir == null)
        {
            String keyspace = ConfigHelper.getOutputKeyspace(conf);
            //dir must be named by ks/cf for the loader
            outputDir = new File(getOutputLocation() + File.separator + keyspace + File.separator + ConfigHelper.getOutputColumnFamily(conf));
            outputDir.mkdirs();
        }
        
        if (writer == null)
        {
            AbstractType<?> subcomparator = null;

            if (cfType == CFType.SUPER)
                subcomparator = BytesType.instance;

            writer = new SSTableSimpleUnsortedWriter(
                    outputDir,
                    ConfigHelper.getOutputPartitioner(conf),
                    ConfigHelper.getOutputKeyspace(conf),
                    ConfigHelper.getOutputColumnFamily(conf),
                    BytesType.instance,
                    subcomparator,
                    Integer.parseInt(conf.get(BUFFER_SIZE_IN_MB, "64")),
                    ConfigHelper.getOutputCompressionParamaters(conf));

            this.loader = new SSTableLoader(outputDir, new ExternalClient(conf), new NullOutputHandler());
        }
    }

    @Override
    public void write(ByteBuffer keybuff, List<Mutation> value) throws IOException
    {
        setTypes(value.get(0));
        prepareWriter();
        SSTableSimpleUnsortedWriter ssWriter = (SSTableSimpleUnsortedWriter) writer;
        ssWriter.newRow(keybuff);
        for (Mutation mut : value)
        {
            if (cfType == CFType.SUPER)
            {
                ssWriter.newSuperColumn(mut.getColumn_or_supercolumn().getSuper_column().name);
                if (colType == ColType.COUNTER)
                    for (CounterColumn column : mut.getColumn_or_supercolumn().getCounter_super_column().columns)
                        ssWriter.addCounterColumn(column.name, column.value);
                else
                {
                    for (Column column : mut.getColumn_or_supercolumn().getSuper_column().columns)
                    {
                        if(column.ttl == 0)
                            ssWriter.addColumn(column.name, column.value, column.timestamp);
                        else
                            ssWriter.addExpiringColumn(column.name, column.value, column.timestamp, column.ttl, System.currentTimeMillis() + ((long)column.ttl * 1000));
                    }
                }
            }
            else
            {
                if (colType == ColType.COUNTER)
                    ssWriter.addCounterColumn(mut.getColumn_or_supercolumn().counter_column.name, mut.getColumn_or_supercolumn().counter_column.value);
                else
                {
                    if(mut.getColumn_or_supercolumn().column.ttl == 0)
                        ssWriter.addColumn(mut.getColumn_or_supercolumn().column.name, mut.getColumn_or_supercolumn().column.value, mut.getColumn_or_supercolumn().column.timestamp);
                    else
                        ssWriter.addExpiringColumn(mut.getColumn_or_supercolumn().column.name, mut.getColumn_or_supercolumn().column.value, mut.getColumn_or_supercolumn().column.timestamp, mut.getColumn_or_supercolumn().column.ttl, System.currentTimeMillis() + ((long)(mut.getColumn_or_supercolumn().column.ttl) * 1000));
                }
            }
            if (null != progress)
                progress.progress();
            if (null != context)
                HadoopCompat.progress(context);
        }
    }
}
