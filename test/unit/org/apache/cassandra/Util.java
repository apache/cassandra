package org.apache.cassandra;

import org.apache.cassandra.db.Column;

public class Util
{
    public static Column column(String name, String value, long timestamp)
    {
        return new Column(name, value.getBytes(), timestamp);
    }
}
