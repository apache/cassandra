package org.apache.cassandra.utils;


/**
 * CRC Factory that uses our pure java crc for default
 */
public class CRC32Factory extends com.github.tjake.CRC32Factory
{
    public static final CRC32Factory instance = new CRC32Factory();

    public CRC32Factory()
    {
        super(PureJavaCrc32.class);
    }
}
