/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.    See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.    The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.    You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.    See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.hadoop.streaming;

import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.avro.Mutation;
import org.apache.cassandra.avro.StreamingMutation;
import org.apache.cassandra.hadoop.ConfigHelper;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.streaming.io.OutputReader;
import org.apache.hadoop.streaming.PipeMapRed;

/**
 * An OutputReader that reads sequential StreamingMutations (from Cassandra's Avro client API), and converts them to
 * the objects used by CassandraOutputFormat. This allows Hadoop Streaming to output efficiently to Cassandra via
 * a familiar API.
 *
 * Avro requires the reader's and writer's schema: otherwise, it assumes they are the same.
 * If the canonical schema that the Cassandra side uses changes, and somebody packaged the {{avpr}}
 * up in their application somehow, or generated code, they'd see a runtime failure.
 * We could allow specifying an alternate Avro schema using a Configuration property to work around this.
 */
public class AvroOutputReader extends OutputReader<ByteBuffer, List<Mutation>>
{
    private BinaryDecoder decoder;
    private SpecificDatumReader<StreamingMutation> reader;

    // reusable values
    private final StreamingMutation entry = new StreamingMutation();
    private final ArrayList<Mutation> mutations = new ArrayList<Mutation>(1);

    @Override
    public void initialize(PipeMapRed pmr) throws IOException
    {
        super.initialize(pmr);

        // set up decoding around the DataInput (hmm) provided by streaming
        InputStream in;
        if (pmr.getClientInput() instanceof InputStream)
            // let's hope this is the case
            in = (InputStream)pmr.getClientInput();
        else
            // ...because this is relatively slow
            in = new FromDataInputStream(pmr.getClientInput());
        decoder = DecoderFactory.defaultFactory().createBinaryDecoder(in, null);
        reader = new SpecificDatumReader<StreamingMutation>(StreamingMutation.SCHEMA$);
    }
    
    @Override
    public boolean readKeyValue() throws IOException
    {
        try
        {
            reader.read(entry, decoder);
        }
        catch (EOFException e)
        {
            return false;
        }
        mutations.clear();
        mutations.add(entry.mutation);
        return true;
    }
    
    @Override
    public ByteBuffer getCurrentKey() throws IOException
    {
        return entry.key;
    }
    
    @Override
    public List<Mutation> getCurrentValue() throws IOException
    {
        return mutations;
    }

    @Override
    public String getLastOutput()
    {
        return entry.toString();
    }
    
    /**
     * Wraps a DataInput to extend InputStream. The exception handling in read() is likely to be ridiculous slow.
     */
    private static final class FromDataInputStream extends InputStream
    {
        private final DataInput in;

        public FromDataInputStream(DataInput in)
        {
            this.in = in;
        }

        @Override
        public boolean markSupported()
        {
            return false;
        }

        @Override
        public int read() throws IOException
        {
            try
            {
                return in.readUnsignedByte();
            }
            catch (EOFException e)
            {
                return -1;
            }
        }

        @Override
        public long skip(long n) throws IOException
        {
            long skipped = 0;
            while (n > 0)
            {
                // skip in batches up to max_int in size
                int skip = (int)Math.min(Integer.MAX_VALUE, n);
                skipped += in.skipBytes(skip);
                n -= skip;
            }
            return skipped;
        }
    }
}
