package org.apache.cassandra.stress.generatedata;
/*
 * 
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
 * 
 */


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.math3.distribution.EnumeratedDistribution;
import org.apache.commons.math3.util.Pair;

import static com.google.common.base.Charsets.UTF_8;

public class DataGenStringDictionary extends DataGen
{

    private final byte space = ' ';
    private final EnumeratedDistribution<byte[]> words;

    public DataGenStringDictionary(EnumeratedDistribution<byte[]> wordDistribution)
    {
        words = wordDistribution;
    }

    @Override
    public void generate(ByteBuffer fill, long index, ByteBuffer seed)
    {
        fill(fill);
    }

    @Override
    public void generate(List<ByteBuffer> fills, long index, ByteBuffer seed)
    {
        for (int i = 0 ; i < fills.size() ; i++)
            fill(fills.get(0));
    }

    private void fill(ByteBuffer fill)
    {
        fill.clear();
        byte[] trg = fill.array();
        int i = 0;
        while (i < trg.length)
        {
            if (i > 0)
                trg[i++] = space;
            byte[] src = words.sample();
            System.arraycopy(src, 0, trg, i, Math.min(src.length, trg.length - i));
            i += src.length;
        }
    }

    @Override
    public boolean isDeterministic()
    {
        return false;
    }

    public static DataGenFactory getFactory(File file) throws IOException
    {
        final List<Pair<byte[], Double>> words = new ArrayList<>();
        try (final BufferedReader reader = new BufferedReader(new FileReader(file)))
        {
            String line;
            while ( null != (line = reader.readLine()) )
            {
                String[] pair = line.split(" +");
                if (pair.length != 2)
                    throw new IllegalArgumentException("Invalid record in dictionary: \"" + line + "\"");
                words.add(new Pair<>(pair[1].getBytes(UTF_8), Double.parseDouble(pair[0])));
            }
            final EnumeratedDistribution<byte[]> dist = new EnumeratedDistribution<byte[]>(words);
            return new DataGenFactory()
            {
                @Override
                public DataGen get()
                {
                    return new DataGenStringDictionary(dist);
                }
            };
        }
    }

}
