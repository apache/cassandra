package org.apache.cassandra.db;

import org.apache.cassandra.ServerTest;
import org.apache.cassandra.service.StorageService;
import org.testng.annotations.Test;

import java.io.IOException;

public class SystemTableTest extends ServerTest {
    @Test
    public void testMain() throws IOException {
        SystemTable.openSystemTable(SystemTable.cfName_).updateToken( StorageService.token("503545744:0") );
    }
}
