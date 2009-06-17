package org.apache.cassandra.db;

import java.io.IOException;

import org.junit.Test;

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.service.StorageService;

public class SystemTableTest extends CleanupHelper
{
    @Test
    public void testMain() throws IOException {
        SystemTable.openSystemTable(SystemTable.cfName_).updateToken(StorageService.getPartitioner().getInitialToken("503545744:0"));
    }
}
