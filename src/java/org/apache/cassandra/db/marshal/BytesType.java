package org.apache.cassandra.db.marshal;

public class BytesType extends AbstractType
{
    public int compare(byte[] o1, byte[] o2)
    {
        int length = Math.max(o1.length, o2.length);
        for (int i = 0; i < length; i++)
        {
            int index = i + 1;
            if (index > o1.length && index <= o2.length)
            {
                return -1;
            }
            if (index > o2.length && index <= o1.length)
            {
                return 1;
            }

            int delta = o1[i] - o2[i];
            if (delta != 0)
            {
                return delta;
            }
        }
        return 0;
    }

    public String getString(byte[] bytes)
    {
        return bytes.toString();
    }
}
