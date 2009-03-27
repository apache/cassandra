package org.apache.cassandra.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import java.util.Comparator;
import java.util.Arrays;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.collections.Predicate;

import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.db.IdentityFilter;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.FileStruct;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.Memtable;
import org.apache.cassandra.db.MemtableManager;
import org.apache.cassandra.io.SequenceFile;
import org.apache.cassandra.io.DataInputBuffer;
import org.apache.cassandra.io.DataOutputBuffer;
import org.apache.cassandra.config.DatabaseDescriptor;

public class RangeVerbHandler implements IVerbHandler
{
    public static final Comparator<String> STRING_COMPARATOR = new Comparator<String>()
    {
        public int compare(String o1, String o2)
        {
            return o1.compareTo(o2);
        }
    };

    public void doVerb(Message message)
    {
        byte[] bytes = (byte[]) message.getMessageBody()[0];
        final String startkey;
        if (bytes.length == 0)
        {
            startkey = "";
        }
        else
        {
            DataInputBuffer dib = new DataInputBuffer();
            dib.reset(bytes, bytes.length);
            try
            {
                startkey = dib.readUTF();
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        List<Iterator<String>> iterators = new ArrayList<Iterator<String>>();
        Table table = Table.open(DatabaseDescriptor.getTableName());
        for (String cfName : table.getApplicationColumnFamilies())
        {
            ColumnFamilyStore cfs = table.getColumnFamilyStore(cfName);

            // memtable keys: current and historical
            Iterator<Memtable> it = (Iterator<Memtable>) IteratorUtils.chainedIterator(
                    IteratorUtils.singletonIterator(cfs.getMemtable()),
                    MemtableManager.instance().getUnflushedMemtables(cfName).iterator());
            while (it.hasNext())
            {
                iterators.add(IteratorUtils.filteredIterator(it.next().sortedKeyIterator(), new Predicate()
                {
                    public boolean evaluate(Object key)
                    {
                        return ((String) key).compareTo(startkey) >= 0;
                    }
                }));
            }

            // sstables
            for (String filename : cfs.getSSTableFilenames())
            {
                try
                {
                    FileStruct fs = new FileStruct(SequenceFile.reader(filename));
                    fs.seekTo(startkey);
                    iterators.add(fs.iterator());
                }
                catch (FileNotFoundException e)
                {
                    throw new RuntimeException(e);
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
            }
        }

        Iterator<String> iter = IteratorUtils.collatedIterator(STRING_COMPARATOR, iterators);
        List<String> keys = new ArrayList<String>();
        String last = null, current = null;

        while (keys.size() < 1000)
        {
            if (!iter.hasNext())
            {
                break;
            }
            current = iter.next();
            if (!current.equals(last))
            {
                last = current;
                for (String cfName : table.getApplicationColumnFamilies())
                {
                    ColumnFamilyStore cfs = table.getColumnFamilyStore(cfName);
                    try
                    {
                        ColumnFamily cf = cfs.getColumnFamily(current, cfName, new IdentityFilter());
                        if (cf != null && cf.getColumns().size() > 0)
                        {
                            keys.add(current);
                            break;
                        }
                    }
                    catch (IOException e)
                    {
                        throw new RuntimeException();
                    }
                }
            }
        }

        DataOutputBuffer dob = new DataOutputBuffer();
        for (String key : keys)
        {
            try
            {
                dob.writeUTF(key);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }
        byte[] data = Arrays.copyOf(dob.getData(), dob.getLength());
        Message response = message.getReply(StorageService.getLocalStorageEndPoint(), data);
        MessagingService.getMessagingInstance().sendOneWay(response, message.getFrom());
    }
}
