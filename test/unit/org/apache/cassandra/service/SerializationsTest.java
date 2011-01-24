package org.apache.cassandra.service;

import org.apache.cassandra.AbstractSerializationsTester;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MerkleTree;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SerializationsTest extends AbstractSerializationsTester
{
    private void testTreeRequestWrite() throws IOException
    {
        DataOutputStream out = getOutput("service.TreeRequest.bin");
        AntiEntropyService.TreeRequestVerbHandler.SERIALIZER.serialize(Statics.req, out);
        Message.serializer().serialize(AntiEntropyService.TreeRequestVerbHandler.makeVerb(Statics.req), out);
        out.close();
    }
    
    @Test
    public void testTreeRequestRead() throws IOException
    {
        if (EXECUTE_WRITES)
            testTreeRequestWrite();
        
        DataInputStream in = getInput("service.TreeRequest.bin");
        assert AntiEntropyService.TreeRequestVerbHandler.SERIALIZER.deserialize(in) != null;
        assert Message.serializer().deserialize(in) != null;
        in.close();
    }
    
    private void testTreeResponseWrite() throws IOException
    {
        AntiEntropyService.Validator v0 = new AntiEntropyService.Validator(Statics.req);
        IPartitioner part = new RandomPartitioner();
        MerkleTree mt = new MerkleTree(part, MerkleTree.RECOMMENDED_DEPTH, Integer.MAX_VALUE);
        List<Token> tokens = new ArrayList<Token>();
        for (int i = 0; i < 10; i++)
        {
            Token t = part.getRandomToken();
            tokens.add(t);
            mt.split(t);
        }
        AntiEntropyService.Validator v1 = new AntiEntropyService.Validator(Statics.req, mt);
        DataOutputStream out = getOutput("service.TreeResponse.bin");
        AntiEntropyService.TreeResponseVerbHandler.SERIALIZER.serialize(v0, out);
        AntiEntropyService.TreeResponseVerbHandler.SERIALIZER.serialize(v1, out);
        Message.serializer().serialize(AntiEntropyService.TreeResponseVerbHandler.makeVerb(FBUtilities.getLocalAddress(), v0), out);
        Message.serializer().serialize(AntiEntropyService.TreeResponseVerbHandler.makeVerb(FBUtilities.getLocalAddress(), v1), out);
        out.close();
    }
    
    @Test
    public void testTreeResponseRead() throws IOException
    {
        if (EXECUTE_WRITES)
            testTreeResponseWrite();
        
        DataInputStream in = getInput("service.TreeResponse.bin");
        assert AntiEntropyService.TreeResponseVerbHandler.SERIALIZER.deserialize(in) != null;
        assert AntiEntropyService.TreeResponseVerbHandler.SERIALIZER.deserialize(in) != null;
        assert Message.serializer().deserialize(in) != null;
        assert Message.serializer().deserialize(in) != null;
        in.close();
    }
    
    private static class Statics
    {
        private static final AntiEntropyService.CFPair pair = new AntiEntropyService.CFPair("Keyspace1", "Standard1");
        private static final AntiEntropyService.TreeRequest req = new AntiEntropyService.TreeRequest("sessionId", FBUtilities.getLocalAddress(), pair);
    }
}
