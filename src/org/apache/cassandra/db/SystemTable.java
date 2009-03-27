/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db;

import java.io.IOException;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.DataInputBuffer;
import org.apache.cassandra.io.DataOutputBuffer;
import org.apache.cassandra.io.IFileReader;
import org.apache.cassandra.io.IFileWriter;
import org.apache.cassandra.io.SequenceFile;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.LogUtil;
import org.apache.log4j.Logger;

/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class SystemTable
{
    private static Logger logger_ = Logger.getLogger(SystemTable.class);
    private static Map<String, SystemTable> instances_ = new HashMap<String, SystemTable>();

    /* Name of the SystemTable */
    public static final String name_ = "System";
    /* Name of the only column family in the Table */
    static final String cfName_ = "LocationInfo";
    /* Name of columns in this table */
    static final String generation_ = "Generation";
    static final String token_ = "Token";

    /* The ID associated with this column family */
    static final int cfId_ = -1;

    /* Table name. */
    private String table_;
    /* after the header position */
    private long startPosition_ = 0L;
    /* Cache the SystemRow that we read. */
    private Row systemRow_;

    /* Use the following writer/reader to write/read to System table */
    private IFileWriter writer_;
    private IFileReader reader_;

    public static SystemTable openSystemTable(String tableName) throws IOException
    {
        SystemTable table = instances_.get("System");
        if ( table == null )
        {
            table = new SystemTable(tableName);
            instances_.put(tableName, table);
        }
        return table;
    }

    SystemTable(String table) throws IOException
    {
        table_ = table;
        String systemTable = getFileName();
        writer_ = SequenceFile.writer(systemTable);
        reader_ = SequenceFile.reader(systemTable);
    }

    private String getFileName()
    {
        return DatabaseDescriptor.getMetadataDirectory() + System.getProperty("file.separator") + table_ + ".db";
    }

    /*
     * Selects the row associated with the given key.
    */
    public Row get(String key) throws IOException
    {
        String file = getFileName();
        DataOutputBuffer bufOut = new DataOutputBuffer();
        reader_.next(bufOut);

        if ( bufOut.getLength() > 0 )
        {
            DataInputBuffer bufIn = new DataInputBuffer();
            bufIn.reset(bufOut.getData(), bufOut.getLength());
            /*
             * This buffer contains key and value so we need to strip
             * certain parts
           */
            // read the key
            bufIn.readUTF();
            // read the data length and then deserialize
            bufIn.readInt();
            try
            {
                systemRow_ = Row.serializer().deserialize(bufIn);
            }
            catch ( IOException e )
            {
                logger_.debug( LogUtil.throwableToString(e) );
            }
        }
        return systemRow_;
    }

    /*
     * This is a one time thing and hence we do not need
     * any commit log related activity. Just write in an
     * atomic fashion to the underlying SequenceFile.
    */
    void apply(Row row) throws IOException
    {
        systemRow_ = row;
        String file = getFileName();
        long currentPos = writer_.getCurrentPosition();
        DataOutputBuffer bufOut = new DataOutputBuffer();
        Row.serializer().serialize(row, bufOut);
        try
        {
            writer_.append(row.key(), bufOut);
        }
        catch ( IOException e )
        {
            writer_.seek(currentPos);
            throw e;
        }
    }

    /*
     * This method is used to update the SystemTable with the
     * new token.
    */
    public void updateToken(BigInteger token) throws IOException
    {
        if ( systemRow_ != null )
        {
            Map<String, ColumnFamily> columnFamilies = systemRow_.getColumnFamilyMap();
            /* Retrieve the "LocationInfo" column family */
            ColumnFamily columnFamily = columnFamilies.get(SystemTable.cfName_);
            long oldTokenColumnTimestamp = columnFamily.getColumn(SystemTable.token_).timestamp();
            /* create the "Token" whose value is the new token. */
            IColumn tokenColumn = new Column(SystemTable.token_, token.toByteArray(), oldTokenColumnTimestamp + 1);
            /* replace the old "Token" column with this new one. */
            logger_.debug("Replacing old token " + new BigInteger( columnFamily.getColumn(SystemTable.token_).value() ).toString() + " with token " + token.toString());
            columnFamily.addColumn(SystemTable.token_, tokenColumn);
            reset(systemRow_);
        }
    }

    public void reset(Row row) throws IOException
    {
        writer_.seek(startPosition_);
        apply(row);
    }

    void delete(Row row) throws IOException
    {
        throw new UnsupportedOperationException("This operation is not supported for System tables");
    }

    public static void main(String[] args) throws Throwable
    {
        LogUtil.init();
        StorageService.instance().start();
        SystemTable.openSystemTable(SystemTable.cfName_).updateToken( StorageService.hash("503545744:0") );
        System.out.println("Done");

        /*
        BigInteger hash = StorageService.hash("304700067:0");
        List<Range> ranges = new ArrayList<Range>();
        ranges.add( new Range(new BigInteger("1218069462158869448693347920504606362273788442553"), new BigInteger("1092770595533781724218060956188429069")) );
        if ( Range.isKeyInRanges(ranges, "304700067:0") )
        {
            System.out.println("Done");
        }
        */
    }
}
