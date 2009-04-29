package org.apache.cassandra.dht;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.cassandra.io.ICompactSerializer;
import org.apache.cassandra.service.StorageService;

public abstract class Token<T extends Comparable> implements Comparable<Token<T>>
{
    private static final TokenSerializer serializer = new TokenSerializer();
    public static TokenSerializer serializer()
    {
        return serializer;
    }

    T token;

    protected Token(T token)
    {
        this.token = token;
    }

    /**
     * This determines the comparison for node destination purposes.
     */
    public int compareTo(Token<T> o)
    {
        return token.compareTo(o.token);
    }

    public String toString()
    {
        return "Token(" + token + ")";
    }

    public boolean equals(Object obj)
    {
        if (!(obj instanceof Token)) {
            return false;
        }
        return token.equals(((Token)obj).token);
    }

    public int hashCode()
    {
        return token.hashCode();
    }

    public static abstract class TokenFactory<T extends Comparable>
    {
        public abstract byte[] toByteArray(Token<T> token);
        public abstract Token<T> fromByteArray(byte[] bytes);
        public abstract Token<T> fromString(String string);
    }

    public static class TokenSerializer implements ICompactSerializer<Token>
    {
        public void serialize(Token token, DataOutputStream dos) throws IOException
        {
            IPartitioner p = StorageService.getPartitioner();
            byte[] b = p.getTokenFactory().toByteArray(token);
            dos.writeInt(b.length);
            dos.write(b);
        }

        public Token deserialize(DataInputStream dis) throws IOException
        {
            IPartitioner p = StorageService.getPartitioner();
            int size = dis.readInt();
            byte[] bytes = new byte[size];
            dis.readFully(bytes);
            return p.getTokenFactory().fromByteArray(bytes);
        }
    }
}
