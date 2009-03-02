/**
 * 
 */
package org.apache.cassandra.loader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.io.SSTable;
import org.apache.cassandra.locator.EndPointSnitch;
import org.apache.cassandra.net.EndPoint;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.LogUtil;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.cassandra.utils.*;


/**
 * This class is used to load the storage endpoints with the relevant data
 * The data should be both what they are responsible for and what should be replicated on the specific
 * endpoints.
 * Population is done based on a xml file which should adhere to a schema.
 *
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */
public class Loader
{
	private static long siesta_ = 60*1000;
    private static Logger logger_ = Logger.getLogger( Loader.class );
	private Importer importer_;
    private StorageService storageService_;
    
    public Loader(StorageService storageService)
    {
        storageService_ = storageService;
    }
    
    /*
     * This method loads all the keys into a special column family 
     * called "RecycleBin". This column family is used for temporary
     * processing of data and then can be recycled. The idea is that 
     * after the load is complete we have all the keys in the system.
     * Now we force a compaction and examine the single Index file 
     * that is generated to determine how the nodes need to relocate
     * to be perfectly load balanced.
     * 
     *  param @ rootDirectory - rootDirectory at which the parsing begins.
     *  param @ table - table that will be populated.
     *  param @ cfName - name of the column that will be populated. This is 
     *  passed in so that we do not unncessary allocate temporary String objects.
    */
    private void preParse(File rootDirectory, String table, String cfName) throws Throwable
    {        
        File[] files = rootDirectory.listFiles();
        
        for ( File file : files )
        {
            if ( file.isDirectory() )
                preParse(file, table, cfName);
            else
            {
                String fileName = file.getName();
                RowMutation rm = new RowMutation(table, fileName);
                rm.add(cfName, fileName.getBytes(), 0);
                rm.apply();
            }
        }
    }
    
    /*
     * Merges a list of strings with a particular combiner.
     */
    String merge( List<String> listFields,  String combiner)
    {
    	if(listFields.size() == 0 )
    		return null;
    	if(listFields.size() == 1)
    		return listFields.get(0);
    	String mergedKey = null;
    	for(String field: listFields)
    	{
    		if(mergedKey == null)
    		{
    			mergedKey = field;
    		}
    		else
    		{
    			mergedKey = mergedKey + combiner + field;
    		}
    	}
    	return mergedKey;
    	
    }
    
    /*
     * This function checks if the local storage endpoint 
     * is reponsible for storing this key .
     */
    boolean checkIfProcessKey(String key)
    {
		EndPoint[] endPoints = storageService_.getNStorageEndPoint(key);
    	EndPoint localEndPoint = StorageService.getLocalStorageEndPoint();
    	for(EndPoint endPoint : endPoints)
    	{
    		if(endPoint.equals(localEndPoint))
    			return true;
    	}
    	return false;
    }
    
   /*
    * This functions parses each file based on delimiters specified in the 
    * xml file. It also looks at all the parameters specified in teh xml and based
    * on that populates the internal Row structure.
    */ 
    void parse(String filepath) throws Throwable
    {
        BufferedReader bufReader = new BufferedReader(new InputStreamReader(
                new FileInputStream(filepath)), 16 * 1024 * 1024);
        String line = null;
        String delimiter_ = new String(",");
        RowMutation rm = null;
        Map<String, RowMutation> rms = new HashMap<String, RowMutation>();
        if(importer_.columnFamily.delimiter != null)
        {
        	delimiter_ = importer_.columnFamily.delimiter;
        }
        while ((line = bufReader.readLine()) != null)
        {
            StringTokenizer st = new StringTokenizer(line, delimiter_);
            List<String> tokenList = new ArrayList<String>();
            String key = null;
            while (st.hasMoreElements())
            {
            	tokenList.add((String)st.nextElement());
            }
            /* Construct the Key */
            List<String> keyFields = new ArrayList<String> ();
            for(int fieldId: importer_.key.fields.field)
            {
            	keyFields.add(tokenList.get(fieldId));
            }
            key = merge(keyFields, importer_.key.combiner);
            if(importer_.key.optimizeIt != null && !importer_.key.optimizeIt)
            {
	            if(!checkIfProcessKey(key))
	            {
	            	continue;
	            }
            }
            rm = rms.get(key);
            if( rm == null)
            {
            	rm = new RowMutation(importer_.table, key);
            	rms.put(key, rm);
            }
            if(importer_.columnFamily.superColumn != null)
            {
            	List<String> superColumnList = new ArrayList<String>();
            	for(int fieldId : importer_.columnFamily.superColumn.fields.field)
            	{
            		superColumnList.add(tokenList.get(fieldId));
            	}
            	String superColumnName = merge(superColumnList, " ");
            	superColumnList.clear();
            	if(importer_.columnFamily.superColumn.tokenize)
            	{
            	    Analyzer analyzer = new StandardAnalyzer();
            	    TokenStream ts = analyzer.tokenStream("superColumn", new StringReader(superColumnName));
            	    Token token = null;
            	    token = ts.next();
            	    while(token != null)
            	    {
            	    	superColumnList.add(token.termText());
                	    token = ts.next();
            	    }
            	}
            	else
            	{
            		superColumnList.add(superColumnName);
            	}
            	for(String sName : superColumnList)
            	{
            		String cfName = importer_.columnFamily.name + ":" + sName;
    	            if(importer_.columnFamily.column != null)
    	            {
    	            	for(ColumnType column : importer_.columnFamily.column )
    	            	{
    	            		String cfColumn = cfName +":" + (column.name == null ? tokenList.get(column.field):column.name);
    	            		rm.add(cfColumn, tokenList.get(column.value.field).getBytes(), Integer.parseInt(tokenList.get(column.timestamp.field)));
    	            	}
    	            }
            		
            	}
            	
            }
            else
            {
	            if(importer_.columnFamily.column != null)
	            {
	            	for(ColumnType column : importer_.columnFamily.column )
	            	{
	            		String cfColumn = importer_.columnFamily.name +":" + (column.name == null ? tokenList.get(column.field):column.name);
	            		rm.add(cfColumn, tokenList.get(column.value.field).getBytes(), Integer.parseInt(tokenList.get(column.timestamp.field)));
	            	}
	            }
            }
        }
        // Now apply the data for all keys  
        // TODO : Add checks for large data
        // size maybe we want to check the 
        // data size and then apply.
        Set<String> keys = rms.keySet();
        for(String pKey : keys)
        {
        	rm = rms.get(pKey);
        	if( rm != null)
        	{
        		rm.apply();
        	}
        }
    }
    
    
    void parseFileList(File dir) 
    {
		int fileCount = dir.list().length;
		for ( int i = 0 ; i < fileCount ; i++ ) 
		{
			File file = new File(dir.list()[i]);
			if ( file.isDirectory())
			{
				parseFileList(file);
			}
			else 
			{
				try
				{
					if(importer_.key.optimizeIt != null && importer_.key.optimizeIt)
					{
						if(checkIfProcessKey(dir.list()[i]))
						{
							parse(dir.listFiles()[i].getAbsolutePath());
						}
					}
					else
					{
						parse(dir.listFiles()[i].getAbsolutePath());
					}
				}
				catch ( Throwable ex ) 
				{
					logger_.error(ex.toString());
				}
			}
		}
    }
	
    void preLoad(File rootDirectory) throws Throwable
    {
        String table = DatabaseDescriptor.getTables().get(0);
        String cfName = Table.recycleBin_ + ":" + "Keys";
        /* populate just the keys. */
        preParse(rootDirectory, table, cfName);
        /* dump the memtables */
        Table.open(table).flush(false);
        /* force a compaction of the files. */
        Table.open(table).forceCompaction(null,null,null);
        
        /*
         * This is a hack to let everyone finish. Just sleep for
         * a couple of minutes. 
        */
        logger_.info("Taking a nap after forcing a compaction ...");
        Thread.sleep(Loader.siesta_);
        
        /* Figure out the keys in the index file to relocate the node */
        List<String> ssTables = Table.open(table).getAllSSTablesOnDisk();
        /* Load the indexes into memory */
        for ( String df : ssTables )
        {
        	SSTable ssTable = new SSTable(df);
        	ssTable.close();
        }
        /* We should have only one file since we just compacted. */        
        List<String> indexedKeys = SSTable.getIndexedKeys();        
        storageService_.relocate(indexedKeys.toArray( new String[0]) );
        
        /*
         * This is a hack to let everyone relocate and learn about
         * each other. Just sleep for a couple of minutes. 
        */
        logger_.info("Taking a nap after relocating ...");
        Thread.sleep(Loader.siesta_);  
        
        /* 
         * Do the cleanup necessary. Delete all commit logs and
         * the SSTables and reset the load state in the StorageService. 
        */
        SSTable.delete(ssTables.get(0));
//        File commitLogDirectory = new File( DatabaseDescriptor.getLogFileLocation() );
//        FileUtils.delete(commitLogDirectory.listFiles());
        storageService_.resetLoadState();
        logger_.info("Finished all the requisite clean up ...");
    }
    
	void load(String xmlFile) throws Throwable
	{
		try
		{
			JAXBContext jc = JAXBContext.newInstance(this.getClass().getPackage().getName());			
			Unmarshaller u = jc.createUnmarshaller();			
			importer_ = (Importer)u.unmarshal(new FileInputStream( xmlFile ) );
			String directory = importer_.columnFamily.directory;
            File rootDirectory = new File(directory);
            preLoad(rootDirectory);
			parseFileList(rootDirectory);
		}
		catch (Exception e)
		{
			logger_.info(LogUtil.throwableToString(e));
		}
		
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Throwable
	{
		LogUtil.init();
        StorageService s = StorageService.instance();
        s.start();
		Loader loader = new Loader(s);
		loader.load("mbox_importer.xml");
	}

}
