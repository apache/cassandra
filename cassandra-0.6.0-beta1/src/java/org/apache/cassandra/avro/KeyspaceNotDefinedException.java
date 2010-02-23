package org.apache.cassandra.avro;

import org.apache.avro.util.Utf8;

// XXX: This is an analogue to org.apache.cassandra.db.KeyspaceNotDefinedException
@SuppressWarnings("serial")
public class KeyspaceNotDefinedException extends InvalidRequestException {
    
    public KeyspaceNotDefinedException(Utf8 why)
    {
        this.why = why;
    }
}
