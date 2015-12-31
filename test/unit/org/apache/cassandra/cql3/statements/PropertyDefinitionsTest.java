package org.apache.cassandra.cql3.statements;

import org.junit.After;
import org.junit.Test;
import org.junit.Before;

import static org.junit.Assert.assertEquals;

public class PropertyDefinitionsTest {
    
    PropertyDefinitions pd;
    
    @Before
    public void setUp()
    {
        pd = new PropertyDefinitions();
    }
    
    @After
    public void clear()
    {
        pd = null;
    }
    

    @Test
    public void testGetBooleanExistant()
    {
        String key = "one";
        pd.addProperty(key, "1");
        assertEquals(Boolean.TRUE, pd.getBoolean(key, null));
        
        key = "TRUE";
        pd.addProperty(key, "TrUe");
        assertEquals(Boolean.TRUE, pd.getBoolean(key, null));
        
        key = "YES";
        pd.addProperty(key, "YeS");
        assertEquals(Boolean.TRUE, pd.getBoolean(key, null));
   
        key = "BAD_ONE";
        pd.addProperty(key, " 1");
        assertEquals(Boolean.FALSE, pd.getBoolean(key, null));
        
        key = "BAD_TRUE";
        pd.addProperty(key, "true ");
        assertEquals(Boolean.FALSE, pd.getBoolean(key, null));
        
        key = "BAD_YES";
        pd.addProperty(key, "ye s");
        assertEquals(Boolean.FALSE, pd.getBoolean(key, null));
    }
    
    @Test
    public void testGetBooleanNonexistant()
    {
        assertEquals(Boolean.FALSE, pd.getBoolean("nonexistant", Boolean.FALSE));
        assertEquals(Boolean.TRUE, pd.getBoolean("nonexistant", Boolean.TRUE));
    }
    
}
