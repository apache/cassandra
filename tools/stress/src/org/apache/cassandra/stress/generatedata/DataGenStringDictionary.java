package org.apache.cassandra.stress.generatedata;

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
    public void generate(ByteBuffer fill, long index)
    {
        fill(fill, 0);
    }

    @Override
    public void generate(List<ByteBuffer> fills, long index)
    {
        for (int i = 0 ; i < fills.size() ; i++)
            fill(fills.get(0), i);
    }

    private void fill(ByteBuffer fill, int column)
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
        return true;
    }

    public static DataGenFactory getFactory(File file) throws IOException
    {
        final List<Pair<byte[], Double>> words = new ArrayList<>();
        final BufferedReader reader = new BufferedReader(new FileReader(file));
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
