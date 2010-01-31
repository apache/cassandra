package org.apache.cassandra.streaming;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.io.ICompactSerializer;
import org.apache.cassandra.net.CompactEndPointSerializationHelper;

/**
 * This encapsulates information of the list of ranges that a target
 * node requires to be transferred. This will be bundled in a
 * StreamRequestsMessage and sent to nodes that are going to handoff
 * the data.
*/
class StreamRequestMetadata
{
    private static ICompactSerializer<StreamRequestMetadata> serializer_;
    static
    {
        serializer_ = new StreamRequestMetadataSerializer();
    }

    protected static ICompactSerializer<StreamRequestMetadata> serializer()
    {
        return serializer_;
    }

    protected InetAddress target_;
    protected Collection<Range> ranges_;

    StreamRequestMetadata(InetAddress target, Collection<Range> ranges)
    {
        target_ = target;
        ranges_ = ranges;
    }

    public String toString()
    {
        StringBuilder sb = new StringBuilder("");
        sb.append(target_);
        sb.append("------->");
        for ( Range range : ranges_ )
        {
            sb.append(range);
            sb.append(" ");
        }
        return sb.toString();
    }

    private static class StreamRequestMetadataSerializer implements ICompactSerializer<StreamRequestMetadata>
    {
        public void serialize(StreamRequestMetadata srMetadata, DataOutputStream dos) throws IOException
        {
            CompactEndPointSerializationHelper.serialize(srMetadata.target_, dos);
            dos.writeInt(srMetadata.ranges_.size());
            for (Range range : srMetadata.ranges_)
            {
                Range.serializer().serialize(range, dos);
            }
        }

        public StreamRequestMetadata deserialize(DataInputStream dis) throws IOException
        {
            InetAddress target = CompactEndPointSerializationHelper.deserialize(dis);
            int size = dis.readInt();
            List<Range> ranges = (size == 0) ? null : new ArrayList<Range>();
            for( int i = 0; i < size; ++i )
            {
                ranges.add(Range.serializer().deserialize(dis));
            }
            return new StreamRequestMetadata( target, ranges );
        }
    }
}
