package org.apache.cassandra.db;

import org.junit.Test;

import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;

public class SuperColumnTest
{   
    @Test
    public void testMissingSubcolumn() {
    	byte[] val = "sample value".getBytes();
    	SuperColumn sc = new SuperColumn("sc1");
    	sc.addColumn(new Column("col1",val,1L));
    	assertNotNull(sc.getSubColumn("col1"));
    	assertNull(sc.getSubColumn("col2"));
    }
}
