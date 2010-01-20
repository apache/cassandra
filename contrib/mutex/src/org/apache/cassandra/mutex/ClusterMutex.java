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

package org.apache.cassandra.mutex;

import java.util.List;
import java.io.IOException;

import org.apache.cassandra.config.DatabaseDescriptor;

import org.apache.log4j.Logger;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.Watcher.Event.KeeperState;

public class ClusterMutex implements Watcher
{
    private static Logger logger = Logger.getLogger(ClusterMutex.class);

    private static ClusterMutex instance;

    // Lazy on purpose. People who do not want mutex, should not need to have to worry about ZK
    private static class LazyHolder
    {
        private static final ClusterMutex clusterMutex = new ClusterMutex();
    }

    public static ClusterMutex instance()
    {
        return LazyHolder.clusterMutex;
    }

    // this must include hyphen (-) as the last character. substring search relies on it
    private final String LockPrefix = "lock-";

    // if we're disconnected from ZooKeeper server, how many times shall we try the lock
    // operation before giving up
    private final int OperationRetries = 3;

    // how long to sleep between retries. Actual time slept is RetryInterval multiplied by how
    // many times have we already tried.
    private final long RetryInterval = 500L;

    // Session timeout to ZooKeeper
    private final int SessionTimeout = 3000;

    private long lastConnect = 0;

    private ZooKeeper zk = null;
    private String root = null;
    private Integer mutex = null;

    private String hostString = new String();

    private ClusterMutex()
    {
        String zooKeeperRoot = DatabaseDescriptor.getZooKeeperRoot();
        this.root = "/" + ((zooKeeperRoot != null) ? zooKeeperRoot : "");
        mutex = new Integer(1);

        String zooKeeperPort = DatabaseDescriptor.getZooKeeperPort();

        for (String zooKeeper : DatabaseDescriptor.getZooKeepers())
        {
            logger.warn(zooKeeper);
            hostString += (hostString.isEmpty()) ? "" : ",";
            hostString += zooKeeper + ":" + zooKeeperPort;
            logger.warn(hostString);
        }

        try
        {
            connectZooKeeper();
            if (zk.exists(root, false) == null)
            {
                logger.info("Mutex root " + root + " does not exists, creating");
                zk.create(root, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        }
        catch (Exception e)
        {
            throw new RuntimeException("ClusterMutex initialization failed: " + e.getMessage());
        }
    }

    /**
     * Connect to zookeeper server
     */
    private synchronized void connectZooKeeper() throws IOException
    {
        if (zk != null && zk.getState() != ZooKeeper.States.CLOSED)
            return;
        logger.info("Connecting to ZooKeepers: " + hostString);
        zk = new ZooKeeper(hostString, SessionTimeout, this);
    }

    /**
     * close current session and try to connect to zookeeper server
     */
    private synchronized void reestablishZooKeeperSession() throws IOException
    {
        long now = System.currentTimeMillis();

        // let's not flood zookeeper with connection requests
        if ((now - lastConnect) < SessionTimeout)
        {
            if (logger.isTraceEnabled())
                logger.trace("Only " + (now - lastConnect) + "ms passed since last reconnect, not trying again yet");
            return;
        }

        lastConnect = now;

        try
        {
            zk.close();
        }
        catch (Exception e)
        {
            // ignore all exceptions. we're calling this just to make sure ephemeral nodes are
            // deleted. zk might be in an inconsistent state and cause exception.
        }

        connectZooKeeper();
    }

    /**
     * process any events from ZooKeeper. We simply wake up any clients that are waiting for
     * file deletion. Number of clients is usually very small (most likely just one), so no need
     * for any complex logic.
     */
    public void process(WatchedEvent event)
    {
        if (logger.isTraceEnabled())
            logger.trace("Got event " + event.getType() + ", keeper state " + event.getState() + ", path " + event.getPath());

        synchronized (mutex)
        {
            mutex.notifyAll();
        }
    }

    private boolean isConnected()
    {
        return zk.getState() == ZooKeeper.States.CONNECTED;
    }

    /**
     * lock
     *
     * @param lockName lock to be locked for writing. name can be any string, but it must not
     * include slash (/) or any character disallowed by ZooKeeper (see
     * hadoop.apache.org/zookeeper/docs/current/zookeeperProgrammers.html#ch_zkDataModel).
     * @return name of the znode inside zookeeper holding this lock.
     */
    public String lock(String lockName) throws KeeperException, InterruptedException, IOException
    {
        for (int i=1; i<=OperationRetries; i++)
        {
            try
            {
                return lockInternal(lockName);
            }
            catch (KeeperException.SessionExpiredException e)
            {
                logger.warn("ZooKeeper session expired, reconnecting");
                reestablishZooKeeperSession();
            }
            catch (KeeperException.ConnectionLossException e)
            {
                // ZooKeeper handles lost connection automatically, but in order to reset all
                // ephemeral nodes, we close the whole thing.
                logger.warn("ZooKeeper connection lost, reconnecting");
                reestablishZooKeeperSession();
            }

            try
            {
                Thread.sleep(RetryInterval * i);
            }
            catch (InterruptedException ignore)
            {
                // Just fall through to retry
            }
        }

        throw new KeeperException.ConnectionLossException();
    }

    /**
     * creates lock znode in zookeeper under lockPath. Lock name is
     * LockPrefix plus ephemeral sequence number given by zookeeper
     * 
     * @param lockPath name of the lock (directory in zookeeper)
     */
    private String createLockZNode(String lockPath) throws KeeperException, InterruptedException
    {
	String lockZNode = null;

        try
        {
            lockZNode = zk.create(lockPath + "/" + LockPrefix, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        }
        catch (NoNodeException e)
        {
            logger.info(lockPath + " does not exist, creating");
            zk.create(lockPath, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            lockZNode = zk.create(lockPath + "/" + LockPrefix, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        }

	return lockZNode;
    }

    /**
     * lockInteral does the actual locking.
     *
     * @param same as in lock
     */
    private String lockInternal(String lockName) throws KeeperException, InterruptedException
    {
        String lockZNode = null;
        String lockPath = root + "/" + lockName;

	lockZNode = createLockZNode(lockPath);

        if (logger.isTraceEnabled())
            logger.trace("lockZNode created " + lockZNode);

        while (true)
        {
	    // check what is our ID (sequence number at the end of file name added by ZK)
	    int mySeqNum = Integer.parseInt(lockZNode.substring(lockZNode.lastIndexOf('-') + 1));
	    int previousSeqNum = -1;
	    String predessor = null;

            // get all children of lock znode and find the one that is just before us, if
            // any. This must be inside loop, as children might get deleted out of order because
            // of client disconnects. We cannot assume that the file that is in front of us this
            // time, is there next time. It might have been deleted even though earlier files
            // are still there.
            List<String> children = zk.getChildren(lockPath, false);
	    if (children.isEmpty())
	    {
		logger.warn("No children in " + lockPath + " although one was just created. Going to try again");
		lockZNode = createLockZNode(lockPath);
		continue;
	    }
            for (String child : children)
            {
                if (logger.isTraceEnabled())
                    logger.trace("child: " + child);
                int otherSeqNum = Integer.parseInt(child.substring(child.lastIndexOf('-') + 1));
                if (otherSeqNum < mySeqNum && otherSeqNum > previousSeqNum)
                {
                    previousSeqNum = otherSeqNum;
                    predessor = child;
                }
            }

            // our sequence number is smallest, we have the lock
            if (previousSeqNum == -1)
            {
                if (logger.isTraceEnabled())
                    logger.trace("No smaller znode sequences, " + lockZNode + " acquired lock");
                return lockZNode;
            }

            // there is at least one znode before us. wait for it to be deleted.
            synchronized (mutex)
            {
                if (zk.exists(lockPath + "/" + predessor, true) == null)
                {
                    if (logger.isTraceEnabled())
                        logger.trace(predessor + " does not exists, " + lockZNode + " acquired lock");
                    break;
                }
                else if (logger.isTraceEnabled())
                    logger.trace(predessor + " is still here, " + lockZNode + " must wait");

                mutex.wait();

                if (isConnected() == false)
                {
                    logger.info("ZooKeeper disconnected while waiting for lock");
                    throw new KeeperException.ConnectionLossException();
                }
            }
        }

        return lockZNode;
    }

    /**
     * unlock
     *
     * @param lockZNode this MUST be the string returned by lock call. Otherwise there will be
     * chaos.
     */
    public void unlock(String lockZNode)
    {
        assert (lockZNode != null);

        if (logger.isTraceEnabled())
            logger.trace("deleting " + lockZNode);

        try
        {
            zk.delete(lockZNode, -1);
        }
        catch (Exception e)
        {
            // We do not do anything here. The idea is to check that everything goes OK when
            // locking and let unlock always succeed from client's point of view. Ephemeral
            // nodes should be taken care of by ZooKeeper, so ignoring any errors here should
            // not break anything.
        }
    }

}
