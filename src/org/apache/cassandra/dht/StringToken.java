package org.apache.cassandra.dht;

public class StringToken extends Token<String>
{
    protected StringToken(String token)
    {
        super(token);
    }

    public int compareTo(Token<String> o)
    {
        return OrderPreservingPartitioner.collator.compare(this.token, o.token);
    }
}
