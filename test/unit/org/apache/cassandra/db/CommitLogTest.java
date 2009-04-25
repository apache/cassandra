package org.apache.cassandra.db;

import org.testng.annotations.Test;

import java.util.Set;
import java.util.Map;
import java.util.Arrays;
import java.io.File;
import java.io.IOException;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.DataInputBuffer;
import org.apache.cassandra.ServerTest;

public class CommitLogTest extends ServerTest {
    @Test
    public void testMain() throws IOException {
        // TODO this is useless, since it assumes we have a working set of commit logs to parse
        /*
        File logDir = new File(DatabaseDescriptor.getLogFileLocation());
        File[] files = logDir.listFiles();
        Arrays.sort( files, new FileUtils.FileComparator() );

        byte[] bytes = new byte[CommitLogHeader.size(Integer.parseInt(args[0]))];
        for ( File file : files )
        {
            CommitLog clog = new CommitLog( file );
            clog.readCommitLogHeader(file.getAbsolutePath(), bytes);
            DataInputBuffer bufIn = new DataInputBuffer();
            bufIn.reset(bytes, 0, bytes.length);
            CommitLogHeader clHeader = CommitLogHeader.serializer().deserialize(bufIn);

            StringBuilder sb = new StringBuilder("");
            for ( byte b : bytes )
            {
                sb.append(b);
                sb.append(" ");
            }

            System.out.println("FILE:" + file);
            System.out.println(clHeader.toString());
        }
        */
    }
}
