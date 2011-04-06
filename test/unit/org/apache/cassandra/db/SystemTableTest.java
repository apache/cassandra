package org.apache.cassandra.db;

import java.net.InetAddress;
import java.net.UnknownHostException;

import com.google.common.base.Charsets;
import org.junit.Test;

import org.apache.cassandra.dht.BytesToken;
import org.apache.cassandra.utils.ByteBufferUtil;

public class SystemTableTest
{
    @Test
    public void testLocalToken()
    {
        SystemTable.updateToken(new BytesToken(ByteBufferUtil.bytes("token")));
        assert new String(((BytesToken) SystemTable.getSavedToken()).token, Charsets.UTF_8).equals("token");

        SystemTable.updateToken(new BytesToken(ByteBufferUtil.bytes("token2")));
        assert new String(((BytesToken) SystemTable.getSavedToken()).token, Charsets.UTF_8).equals("token2");
    }

    @Test
    public void testNonLocalToken() throws UnknownHostException
    {
        BytesToken token = new BytesToken(ByteBufferUtil.bytes("token3"));
        InetAddress address = InetAddress.getByName("127.0.0.2");
        SystemTable.updateToken(address, token);
        assert SystemTable.loadTokens().get(token).equals(address);
        SystemTable.removeToken(token);
        assert !SystemTable.loadTokens().containsKey(token);
    }
}
