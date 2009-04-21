package org.apache.cassandra.db;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import org.testng.annotations.Test;
public class SuperColumnTest
{   
    @Test
    public void testMissingSubcolumn() {
    	byte[] val = "sample value".getBytes();
    	SuperColumn sc = new SuperColumn("sc1");
    	sc.addColumn("col1", new Column("col1",val,1L));
    	assertNotNull(sc.getSubColumn("col1"));
    	assertNull(sc.getSubColumn("col2"));
    }
}
