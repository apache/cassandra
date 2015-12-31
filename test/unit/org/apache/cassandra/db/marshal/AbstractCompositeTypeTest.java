package org.apache.cassandra.db.marshal;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class AbstractCompositeTypeTest
{
    
    @Test
    public void testEscape()
    {
        assertEquals("", AbstractCompositeType.escape(""));
        assertEquals("Ab!CdXy \\Z123-345", AbstractCompositeType.escape("Ab!CdXy \\Z123-345"));
        assertEquals("Ab!CdXy \\Z123-345!!", AbstractCompositeType.escape("Ab!CdXy \\Z123-345!"));
        assertEquals("Ab!CdXy \\Z123-345\\!", AbstractCompositeType.escape("Ab!CdXy \\Z123-345\\"));
        
        assertEquals("A\\:b!CdXy \\\\:Z123-345", AbstractCompositeType.escape("A:b!CdXy \\:Z123-345"));
        assertEquals("A\\:b!CdXy \\\\:Z123-345!!", AbstractCompositeType.escape("A:b!CdXy \\:Z123-345!"));
        assertEquals("A\\:b!CdXy \\\\:Z123-345\\!", AbstractCompositeType.escape("A:b!CdXy \\:Z123-345\\"));
        
    }
    
    @Test
    public void testUnescape()
    {
        assertEquals("", AbstractCompositeType.escape(""));
        assertEquals("Ab!CdXy \\Z123-345", AbstractCompositeType.unescape("Ab!CdXy \\Z123-345"));
        assertEquals("Ab!CdXy \\Z123-345!", AbstractCompositeType.unescape("Ab!CdXy \\Z123-345!!"));
        assertEquals("Ab!CdXy \\Z123-345\\", AbstractCompositeType.unescape("Ab!CdXy \\Z123-345\\!"));
        
        assertEquals("A:b!CdXy \\:Z123-345", AbstractCompositeType.unescape("A\\:b!CdXy \\\\:Z123-345"));
        assertEquals("A:b!CdXy \\:Z123-345!", AbstractCompositeType.unescape("A\\:b!CdXy \\\\:Z123-345!!"));
        assertEquals("A:b!CdXy \\:Z123-345\\", AbstractCompositeType.unescape("A\\:b!CdXy \\\\:Z123-345\\!"));
    }
}
