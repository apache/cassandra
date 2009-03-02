package org.apache.cassandra.tools;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;

import org.apache.cassandra.net.EndPoint;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;


public class ClusterTool
{
	public static final String SET_TOKEN = "settoken";
	public static final String HASH_KEY = "hash";
	public static final String BUILD_INDEX = "build_index";
	public static final String READ_TEST = "read_test";
	public static final String WRITE_TEST = "write_test";

    public static void applyToken(String serverName, BigInteger token) throws IOException
    {
        try
        {
        	EndPoint from = new EndPoint(InetAddress.getLocalHost().getHostName(), 7000);
        	System.out.println("Updating token of server " + serverName + " with token " + token);
            Message message = new Message(from, "", StorageService.tokenVerbHandler_, new Object[]{ token.toByteArray() });
            EndPoint ep = new EndPoint(serverName, 7000);
        	MessagingService.getMessagingInstance().sendOneWay(message, ep);
        	Thread.sleep(1000);
	    	System.out.println("Successfully calibrated " + serverName);
        }
        catch (Exception e)
        {
            e.printStackTrace(System.out);
        }
    }

    public static void printUsage()
    {
		System.out.println("Usage: java -jar <cassandra-tools.jar> <command> <options>");
		System.out.println("Commands:");
		System.out.println("\t" + SET_TOKEN + " <server> <token>");
		System.out.println("\t" + HASH_KEY + " <key>");
		System.out.println("\t" + BUILD_INDEX + "  <full path to the data file>");
		System.out.println("\t" + READ_TEST + " <number of threads> <requests per sec per thread> <machine(s) to read (':' separated list)>");
		System.out.println("\t" + WRITE_TEST + " <number of threads> <requests per sec per thread> <machine(s) to write (':' separated list)>");
    }

    public static void main(String[] args) throws Exception
    {
    	if(args.length < 2)
    	{
    		printUsage();
    		return;
    	}

    	int argc = 0;
    	try
    	{
    		/* set the token for a particular node in the Cassandra cluster */
	    	if(SET_TOKEN.equals(args[argc]))
	    	{
		    	String serverName = args[argc + 1];
		    	BigInteger token = new BigInteger(args[argc + 2]);
		    	//System.out.println("Calibrating " + serverName + " with token " + token);
		    	applyToken(serverName, token);
	    	}
	    	/* Print the hash of a given key */
	    	else if(HASH_KEY.equals(args[argc]))
	    	{
	    		System.out.println("Hash = [" + StorageService.hash(args[argc + 1]) + "]");
	    	}
	    	/* build indexes given the data file */
	    	else if(BUILD_INDEX.equals(args[argc]))
	    	{
	    		IndexBuilder.main(args);
	    	}
	    	/* test reads */
	    	else if(READ_TEST.equals(args[argc]))
	    	{
	    		System.out.println("Testing reads...");
	    		int numThreads = Integer.parseInt(args[argc + 1]);
				int rpsPerThread = Integer.parseInt(args[argc + 2]);
				String machinesToRead = args[argc + 3];
//				ReadTest.runReadTest(numThreads, rpsPerThread, machinesToRead);
	    	}
	    	/* test writes */
	    	else if(WRITE_TEST.equals(args[argc]))
	    	{
	    		System.out.println("Testing writes...");
	    		int numThreads = Integer.parseInt(args[argc + 1]);
				int rpsPerThread = Integer.parseInt(args[argc + 2]);
				String machinesToWrite = args[argc + 3];
//				WriteTest.runWriteTest(numThreads, rpsPerThread, machinesToWrite);
	    	}
    	} catch(Exception e)
    	{
    		System.err.println("Exception " + e.getMessage());
			e.printStackTrace(System.err);
    		printUsage();
    	}

    	System.exit(0);
    }

}
