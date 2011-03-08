package org.apache.cassandra.db.marshal;

import com.google.common.base.Charsets;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;
import org.junit.Test;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;

public class RoundTripTest
{
    @Test
    public void testInteger()
    {
        BigInteger bi = new BigInteger("1");
        assert bi.intValue() == 1;
        assert IntegerType.instance.getString(IntegerType.instance.fromString("1")).equals("1");
        assert IntegerType.instance.fromString(IntegerType.instance.getString(ByteBuffer.wrap(bi.toByteArray())))
                .equals(ByteBuffer.wrap(bi.toByteArray()));
        assert IntegerType.instance.compose(ByteBuffer.wrap(bi.toByteArray())).equals(bi);
    }
    
    @Test
    public void testLong()
    {
        byte[] v = new byte[]{0,0,0,0,0,0,0,1};
        assert LongType.instance.getString(LongType.instance.fromString("1")).equals("1");
        assert LongType.instance.fromString(LongType.instance.getString(ByteBuffer.wrap(v)))
                .equals(ByteBuffer.wrap(v));
        assert LongType.instance.compose(ByteBuffer.wrap(v)) == 1L;
    }
    
    @Test
    public void testAscii() throws Exception
    {
        byte[] abc = "abc".getBytes(Charsets.US_ASCII);
        assert AsciiType.instance.getString(AsciiType.instance.fromString("abc")).equals("abc");
        assert AsciiType.instance.fromString(AsciiType.instance.getString(ByteBuffer.wrap(abc)))
                .equals(ByteBuffer.wrap(abc));
        assert AsciiType.instance.compose(ByteBuffer.wrap(abc)).equals("abc");
    }
    
    @Test
    public void testBytes()
    {
        byte[] v = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        assert BytesType.instance.getString(BytesType.instance.fromString(FBUtilities.bytesToHex(v)))
                .equals(FBUtilities.bytesToHex(v));
        assert BytesType.instance.fromString(BytesType.instance.getString(ByteBuffer.wrap(v)))
                .equals(ByteBuffer.wrap(v));
        assert BytesType.instance.compose(ByteBuffer.wrap(v)).equals(ByteBuffer.wrap(v));
    }
    
    @Test
    public void testLexicalUUID()
    {
        UUID uuid = UUIDGen.makeType1UUIDFromHost(FBUtilities.getLocalAddress());
        assert LexicalUUIDType.instance.getString(LexicalUUIDType.instance.fromString(uuid.toString()))
                .equals(uuid.toString());
        assert LexicalUUIDType.instance.fromString(LexicalUUIDType.instance.getString(ByteBuffer.wrap(UUIDGen.decompose(uuid))))
                .equals(ByteBuffer.wrap(UUIDGen.decompose(uuid)));
        assert LexicalUUIDType.instance.compose(ByteBuffer.wrap(UUIDGen.decompose(uuid))).equals(uuid);
    }
    
    @Test
    public void testTimeUUID()
    {
        UUID uuid = UUIDGen.makeType1UUIDFromHost(FBUtilities.getLocalAddress());
        assert TimeUUIDType.instance.getString(TimeUUIDType.instance.fromString(uuid.toString()))
                .equals(uuid.toString());
        assert TimeUUIDType.instance.fromString(TimeUUIDType.instance.getString(ByteBuffer.wrap(UUIDGen.decompose(uuid))))
                .equals(ByteBuffer.wrap(UUIDGen.decompose(uuid)));
        assert TimeUUIDType.instance.compose(ByteBuffer.wrap(UUIDGen.decompose(uuid))).equals(uuid);
    }
    
    @Test
    public void testUtf8() throws Exception
    {
        String v = "\u2297\u5432\u2376\u263d\uf543";
        assert UTF8Type.instance.getString(UTF8Type.instance.fromString(v)).equals(v);
        assert UTF8Type.instance.fromString(UTF8Type.instance.getString(ByteBuffer.wrap(v.getBytes(Charsets.UTF_8))))
                .equals(ByteBuffer.wrap(v.getBytes(Charsets.UTF_8)));
        assert UTF8Type.instance.compose(ByteBuffer.wrap(v.getBytes(Charsets.UTF_8))).equals(v);
    }
}
