package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.mockito.Mockito;


public class EmptyTypeTest
{
    @Test
    public void isFixed()
    {
        Assert.assertEquals(0, EmptyType.instance.valueLengthIfFixed());
    }

    @Test
    public void writeEmptyAllowed()
    {
        DataOutputPlus output = Mockito.mock(DataOutputPlus.class);
        EmptyType.instance.writeValue(ByteBufferUtil.EMPTY_BYTE_BUFFER, output);

        Mockito.verifyNoInteractions(output);
    }

    @Test
    public void writeNonEmpty()
    {
        DataOutputPlus output = Mockito.mock(DataOutputPlus.class);
        ByteBuffer rejected = ByteBuffer.wrap("this better fail".getBytes());

        boolean thrown = false;
        try
        {
            EmptyType.instance.writeValue(rejected, output);
        }
        catch (AssertionError e)
        {
            thrown = true;
        }
        Assert.assertTrue("writeValue did not reject non-empty input", thrown);

        Mockito.verifyNoInteractions(output);
    }

    @Test
    public void read()
    {
        DataInputPlus input = Mockito.mock(DataInputPlus.class);

        ByteBuffer buffer = EmptyType.instance.readValue(input);
        Assert.assertNotNull(buffer);
        Assert.assertFalse("empty type returned back non-empty data", buffer.hasRemaining());

        buffer = EmptyType.instance.readValue(input, 42);
        Assert.assertNotNull(buffer);
        Assert.assertFalse("empty type returned back non-empty data", buffer.hasRemaining());

        Mockito.verifyNoInteractions(input);
    }

    @Test
    public void decompose()
    {
        ByteBuffer buffer = EmptyType.instance.decompose(null);
        Assert.assertEquals(0, buffer.remaining());
    }

    @Test
    public void composeEmptyInput()
    {
        Void result = EmptyType.instance.compose(ByteBufferUtil.EMPTY_BYTE_BUFFER);
        Assert.assertNull(result);
    }

    @Test
    public void composeNonEmptyInput()
    {
        try
        {
            EmptyType.instance.compose(ByteBufferUtil.bytes("should fail"));
            Assert.fail("compose is expected to reject non-empty values, but did not");
        }
        catch (MarshalException e) {
            Assert.assertTrue(e.getMessage().startsWith("EmptyType only accept empty values"));
        }
    }
}
