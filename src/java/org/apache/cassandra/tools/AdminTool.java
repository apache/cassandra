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

package org.apache.cassandra.tools;

import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.RowMutationMessage;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.net.EndPoint;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.BasicUtilities;
import org.apache.cassandra.utils.LogUtil;


/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class AdminTool
{

	String server_ = null;
	String tableName_ = "Mailbox";
	String key_ = "Random";
	String cf1_ = "MailboxThreadList0";
	String cf2_ = "MailboxUserList0";
	String cf3_ = "MailboxMailList0";
	String cf4_ = "MailboxMailData0";
//	String cf5_ = "MailboxUserList";
	public static EndPoint from_ = new EndPoint("hadoop071.sf2p.facebook.com", 10001);
	private static final String[] servers_ =
	{
		"insearch001.sf2p.facebook.com",
		"insearch002.sf2p.facebook.com",
		"insearch003.sf2p.facebook.com",
		"insearch004.sf2p.facebook.com",
		"insearch005.sf2p.facebook.com",
		"insearch016.sf2p.facebook.com",
		"insearch007.sf2p.facebook.com",
		"insearch008.sf2p.facebook.com",
		"insearch009.sf2p.facebook.com",
		"insearch010.sf2p.facebook.com",
		"insearch011.sf2p.facebook.com",
		"insearch012.sf2p.facebook.com",
		"insearch013.sf2p.facebook.com",
		"insearch014.sf2p.facebook.com",
		"insearch015.sf2p.facebook.com",
		"insearch016.sf2p.facebook.com",
		"insearch017.sf2p.facebook.com",
		"insearch018.sf2p.facebook.com",
		"insearch019.sf2p.facebook.com",
		"insearch020.sf2p.facebook.com",
		"insearch021.sf2p.facebook.com",
		"insearch022.sf2p.facebook.com",
		"insearch023.sf2p.facebook.com",
		"insearch024.sf2p.facebook.com",
		"insearch025.sf2p.facebook.com",
		"insearch026.sf2p.facebook.com",
		"insearch027.sf2p.facebook.com",
		"insearch028.sf2p.facebook.com",
		"insearch029.sf2p.facebook.com",
		"insearch030.sf2p.facebook.com",
		"insearch031.sf2p.facebook.com",
		"insearch032.sf2p.facebook.com",
		"insearch033.sf2p.facebook.com",
		"insearch034.sf2p.facebook.com",
		"insearch035.sf2p.facebook.com",
		"insearch036.sf2p.facebook.com",
		"insearch037.sf2p.facebook.com",
		"insearch038.sf2p.facebook.com",
		"insearch039.sf2p.facebook.com",
		"insearch040.sf2p.facebook.com",

		"insearch001.ash1.facebook.com",
		"insearch002.ash1.facebook.com",
		"insearch003.ash1.facebook.com",
		"insearch004.ash1.facebook.com",
		"insearch005.ash1.facebook.com",
		"insearch016.ash1.facebook.com",
		"insearch007.ash1.facebook.com",
		"insearch008.ash1.facebook.com",
		"insearch009.ash1.facebook.com",
		"insearch010.ash1.facebook.com",
		"insearch011.ash1.facebook.com",
		"insearch012.ash1.facebook.com",
		"insearch013.ash1.facebook.com",
		"insearch014.ash1.facebook.com",
		"insearch015.ash1.facebook.com",
		"insearch016.ash1.facebook.com",
		"insearch017.ash1.facebook.com",
		"insearch018.ash1.facebook.com",
		"insearch019.ash1.facebook.com",
		"insearch020.ash1.facebook.com",
		"insearch021.ash1.facebook.com",
		"insearch022.ash1.facebook.com",
		"insearch023.ash1.facebook.com",
		"insearch024.ash1.facebook.com",
		"insearch025.ash1.facebook.com",
		"insearch026.ash1.facebook.com",
		"insearch027.ash1.facebook.com",
		"insearch028.ash1.facebook.com",
		"insearch029.ash1.facebook.com",
		"insearch030.ash1.facebook.com",
		"insearch031.ash1.facebook.com",
		"insearch032.ash1.facebook.com",
		"insearch033.ash1.facebook.com",
		"insearch034.ash1.facebook.com",
		"insearch035.ash1.facebook.com",
		"insearch036.ash1.facebook.com",
		"insearch037.ash1.facebook.com",
		"insearch038.ash1.facebook.com",
		"insearch039.ash1.facebook.com",
		"insearch040.ash1.facebook.com",
	};

	AdminTool()
	{
		server_ = null;
	}

	AdminTool(String server)
	{
		server_ = server;
	}

	public void run(int operation, String columnFamilyName, long skip) throws Throwable
	{
        byte[] bytes =  BasicUtilities.longToByteArray( skip );
        RowMutation rm = new RowMutation(tableName_, key_);
        if( columnFamilyName == null )
        {
			rm.add(Table.recycleBin_ + ":" + cf1_, bytes, operation);
			rm.add(Table.recycleBin_ + ":" + cf2_, bytes, operation);
			rm.add(Table.recycleBin_ + ":" + cf3_, bytes, operation);
			rm.add(Table.recycleBin_ + ":" + cf4_, bytes, operation);
//			rm.add(Table.recycleBin_ + ":" + cf5_, bytes, operation);
        }
        else
        {
			rm.add(Table.recycleBin_ + ":" + columnFamilyName, bytes, operation);
        }
		RowMutationMessage rmMsg = new RowMutationMessage(rm);
        if( server_ != null)
        {
            Message message = RowMutationMessage.makeRowMutationMessage(rmMsg, StorageService.binaryVerbHandler_);
	        EndPoint to = new EndPoint(server_, 7000);
			MessagingService.getMessagingInstance().sendOneWay(message, to);
        }
        else
        {
        	for( String server : servers_ )
        	{
                Message message = RowMutationMessage.makeRowMutationMessage(rmMsg, StorageService.binaryVerbHandler_);
		        EndPoint to = new EndPoint(server, 7000);
				MessagingService.getMessagingInstance().sendOneWay(message, to);
        	}
        }
	}


	/**
	 * @param args
	 */
	public static void main(String[] args) throws Throwable
	{
		LogUtil.init();
		AdminTool postLoad = null;
		int operation = 1;
		String columnFamilyName = null;
		long skip = 0L;
		if(args.length < 1 )
		{
			System.out.println("Usage: PostLoad <serverName>  < operation 1- flushBinary 2 - compactions 3- flush> <ColumnFamilyName> <skip factor for compactions> or  PostLoad <-all> <operation> <ColumnFamilyName> <skip factor for compactions>");
		}
		if(args[0].equals("-all"))
		{
			 postLoad = new AdminTool();
		}
		else
		{
			 postLoad = new AdminTool(args[0]);
		}
		if(args.length > 1 )
			operation = Integer.parseInt(args[1]);
		if(args.length > 2 )
			columnFamilyName = args[2];
		if(args.length > 3 )
			skip = Long.parseLong(args[3]);
		postLoad.run(operation, columnFamilyName, skip);

		Thread.sleep(10000);
		System.out.println("Exiting app...");
		System.exit(0);
	}

}
