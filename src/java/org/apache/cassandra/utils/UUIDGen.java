package org.apache.cassandra.utils;

import org.safehaus.uuid.EthernetAddress;
import org.safehaus.uuid.UUIDGenerator;

import java.math.BigInteger;
import java.net.InetAddress;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;
import java.util.UUID;

/**
 * Generates type 1 (time-based) UUIDs
 */
public class UUIDGen
{
    /** creates a type1 uuid but substitutes hash of the IP where the mac would go. */
    public static synchronized UUID makeType1UUIDFromHost(InetAddress addr)
    {
        try
        {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            digest.update(addr.getAddress());
            byte[] md5 = digest.digest();
            byte[] fauxMac = new byte[6];
            System.arraycopy(md5, 0, fauxMac, 0, Math.min(md5.length, fauxMac.length));
            return makeType1UUID(UUIDGenerator.getInstance().generateTimeBasedUUID(new EthernetAddress(fauxMac)).toByteArray());
        }
        catch (NoSuchAlgorithmException ex)
        {
            throw new RuntimeException("Your platform has no support for generating MD5 sums");
        }
    }

    /** creates a type 1 uuid from raw bytes. */
    static UUID makeType1UUID(byte[] raw)
    {
        long most = 0;
        long least = 0;
        assert raw.length == 16;
        for (int i = 0; i < 8; i++)
            most = (most << 8) | (raw[i] & 0xff);
        for (int i =8 ; i < 16; i++)
            least = (least << 8) | (raw[i] & 0xff);
        return new UUID(most, least);
    }

    /** decomposes a uuid into raw bytes. */
    static byte[] decompose(UUID uuid)
    {
        long most = uuid.getMostSignificantBits();
        long least = uuid.getLeastSignificantBits();
        byte[] b = new byte[16];
        for (int i = 0; i < 8; i++)
        {
            b[i] = (byte)(most >>> ((7-i) * 8));
            b[8+i] = (byte)(least >>> ((7-i) * 8));
        }
        return b;
    }
}
