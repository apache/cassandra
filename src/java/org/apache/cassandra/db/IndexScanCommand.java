package org.apache.cassandra.db;

import java.io.*;
import java.util.Arrays;

import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.io.ICompactSerializer2;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.IndexClause;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;

public class IndexScanCommand
{
    private static final IndexScanCommandSerializer serializer = new IndexScanCommandSerializer();

    public final String keyspace;
    public final String column_family;
    public final IndexClause index_clause;
    public final SlicePredicate predicate;

    public IndexScanCommand(String keyspace, String column_family, IndexClause index_clause, SlicePredicate predicate)
    {

        this.keyspace = keyspace;
        this.column_family = column_family;
        this.index_clause = index_clause;
        this.predicate = predicate;
    }

    public Message getMessage()
    {
        DataOutputBuffer dob = new DataOutputBuffer();
        try
        {
            serializer.serialize(this, dob);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
        return new Message(FBUtilities.getLocalAddress(),
                           StageManager.READ_STAGE,
                           StorageService.Verb.INDEX_SCAN,
                           Arrays.copyOf(dob.getData(), dob.getLength()));
    }

    public static IndexScanCommand read(Message message) throws IOException
    {
        byte[] bytes = message.getMessageBody();
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        return serializer.deserialize(new DataInputStream(bis));
    }

    private static class IndexScanCommandSerializer implements ICompactSerializer2<IndexScanCommand>
    {
        public void serialize(IndexScanCommand o, DataOutput out) throws IOException
        {
            out.writeUTF(o.keyspace);
            out.writeUTF(o.column_family);
            TSerializer ser = new TSerializer(new TBinaryProtocol.Factory());
            FBUtilities.serialize(ser, o.index_clause, out);
            FBUtilities.serialize(ser, o.predicate, out);
        }

        public IndexScanCommand deserialize(DataInput in) throws IOException
        {
            String keyspace = in.readUTF();
            String columnFamily = in.readUTF();

            TDeserializer dser = new TDeserializer(new TBinaryProtocol.Factory());
            IndexClause indexClause = new IndexClause();
            FBUtilities.deserialize(dser, indexClause, in);
            SlicePredicate predicate = new SlicePredicate();
            FBUtilities.deserialize(dser, predicate, in);

            return new IndexScanCommand(keyspace, columnFamily, indexClause, predicate);
        }
    }
}
