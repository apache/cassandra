package org.apache.cassandra.schema;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IndexMetadataTest {
    
    @Test
    public void testIsNameValidPositive()
    {
        assertTrue(IndexMetadata.isNameValid("abcdefghijklmnopqrstuvwxyz"));
        assertTrue(IndexMetadata.isNameValid("ABCDEFGHIJKLMNOPQRSTUVWXYZ"));
        assertTrue(IndexMetadata.isNameValid("_01234567890"));
    }
    
    @Test
    public void testIsNameValidNegative()
    {
        assertFalse(IndexMetadata.isNameValid(null));
        assertFalse(IndexMetadata.isNameValid(""));
        assertFalse(IndexMetadata.isNameValid(" "));
        assertFalse(IndexMetadata.isNameValid("@"));
        assertFalse(IndexMetadata.isNameValid("!"));
    }
    
    @Test
    public void testGetDefaultIndexName()
    {
        Assert.assertEquals("aB4__idx", IndexMetadata.getDefaultIndexName("a B-4@!_+", null));
        Assert.assertEquals("34_Ddd_F6_idx", IndexMetadata.getDefaultIndexName("34_()Ddd", "#F%6*"));
        
    }
}
