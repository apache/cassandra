package org.apache.cassandra.cql3;

import com.google.common.io.ByteStreams;
import junit.framework.Assert;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class QueryOptionsSerializationTest
{
    public static List<ByteBuffer> values;
    public static List<String> names;
    static
    {
        values = new ArrayList<>(1);
        values.add(ByteBufferUtil.bytes(4));

        names = new ArrayList<>(1);
        names.add("wow");
    }

    public static QueryOptions.SpecificOptions specificOptions
            = new QueryOptions.SpecificOptions(1, null, ConsistencyLevel.ONE, 123);
    public static QueryOptions.DefaultQueryOptions defaultOptions
            = new QueryOptions.DefaultQueryOptions(ConsistencyLevel.ONE, values, false, specificOptions, 0);
    public static QueryOptions.OptionsWithNames namedOptions = new QueryOptions.OptionsWithNames(defaultOptions, names);

    @Test
    public void specificOptions() throws Exception
    {
        DataOutputBuffer out = new DataOutputBuffer();
        QueryOptions.SpecificOptions.serializer.serialize(specificOptions, out, 0);
        int expectedSize = out.getLength();

        Assert.assertEquals(expectedSize, QueryOptions.SpecificOptions.serializer.serializedSize(specificOptions, 0));
        QueryOptions.SpecificOptions deserialized = QueryOptions.SpecificOptions.serializer.deserialize(ByteStreams.newDataInput(out.getData()), 0);
//        Assert.assertEquals(specificOptions, deserialized);
    }

    @Test
    public void defaultOptions() throws Exception
    {
        DataOutputBuffer out = new DataOutputBuffer();
        QueryOptions.DefaultQueryOptions.serializer.serialize(defaultOptions, out, 0);
        int expectedSize = out.getLength();

        Assert.assertEquals(expectedSize, QueryOptions.DefaultQueryOptions.serializer.serializedSize(defaultOptions, 0));

        QueryOptions.DefaultQueryOptions deserialized =
                (QueryOptions.DefaultQueryOptions) QueryOptions.DefaultQueryOptions.serializer.deserialize(ByteStreams.newDataInput(out.getData()), 0);
//        Assert.assertEquals(defaultOptions, deserialized);
    }

    @Test
    public void defaultOptionsGeneric() throws Exception
    {
        DataOutputBuffer out = new DataOutputBuffer();
        QueryOptions.serializer.serialize(defaultOptions, out, 0);
        int expectedSize = out.getLength();

        Assert.assertEquals(expectedSize, QueryOptions.serializer.serializedSize(defaultOptions, 0));

        QueryOptions.DefaultQueryOptions deserialized =
                (QueryOptions.DefaultQueryOptions) QueryOptions.serializer.deserialize(ByteStreams.newDataInput(out.getData()), 0);
//        Assert.assertEquals(defaultOptions, deserialized);
    }

    @Test
    public void namedOptions() throws Exception
    {
        DataOutputBuffer out = new DataOutputBuffer();
        QueryOptions.OptionsWithNames.serializer.serialize(namedOptions, out, 0);
        int expectedSize = out.getLength();

        Assert.assertEquals(expectedSize, QueryOptions.OptionsWithNames.serializer.serializedSize(namedOptions, 0));

        QueryOptions.OptionsWithNames deserialized =
                (QueryOptions.OptionsWithNames) QueryOptions.OptionsWithNames.serializer.deserialize(ByteStreams.newDataInput(out.getData()), 0);
//        Assert.assertEquals(namedOptions, deserialized);
    }

    @Test
    public void namedOptionsGeneric() throws Exception
    {
        DataOutputBuffer out = new DataOutputBuffer();
        QueryOptions.serializer.serialize(namedOptions, out, 0);
        int expectedSize = out.getLength();

        Assert.assertEquals(expectedSize, QueryOptions.serializer.serializedSize(namedOptions, 0));

        QueryOptions.OptionsWithNames deserialized =
                (QueryOptions.OptionsWithNames) QueryOptions.serializer.deserialize(ByteStreams.newDataInput(out.getData()), 0);
//        Assert.assertEquals(namedOptions, deserialized);
    }
}
