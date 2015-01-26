/*
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
package org.apache.pig.test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.cassandra.utils.FBUtilities;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.MiniMRCluster;

public class MiniCluster extends MiniGenericCluster {
    private MiniMRCluster m_mr = null;
    public MiniCluster() {
        super();
    }

    @Override
    protected void setupMiniDfsAndMrClusters() {
        try {
            System.setProperty("hadoop.log.dir", "build/test/logs");
            final int dataNodes = 4;     // There will be 4 data nodes
            final int taskTrackers = 4;  // There will be 4 task tracker nodes

            // Create the configuration hadoop-site.xml file
            File conf_dir = new File("build/classes/");
            conf_dir.mkdirs();
            File conf_file = new File(conf_dir, "hadoop-site.xml");

            conf_file.delete();

            // Builds and starts the mini dfs and mapreduce clusters
            Configuration config = new Configuration();
            if (FBUtilities.isWindows())
                config.set("fs.file.impl", WindowsLocalFileSystem.class.getName());
            m_dfs = new MiniDFSCluster(config, dataNodes, true, null);
            m_fileSys = m_dfs.getFileSystem();
            m_mr = new MiniMRCluster(taskTrackers, m_fileSys.getUri().toString(), 1);

            // Write the necessary config info to hadoop-site.xml
            m_conf = m_mr.createJobConf();
            m_conf.setInt("mapred.submit.replication", 2);
            m_conf.set("dfs.datanode.address", "0.0.0.0:0");
            m_conf.set("dfs.datanode.http.address", "0.0.0.0:0");
            m_conf.set("mapred.map.max.attempts", "2");
            m_conf.set("mapred.reduce.max.attempts", "2");
            m_conf.set("pig.jobcontrol.sleep", "100");
            try (OutputStream os = new FileOutputStream(conf_file))
            {
                m_conf.writeXml(os);
            }

            // Set the system properties needed by Pig
            System.setProperty("cluster", m_conf.get("mapred.job.tracker"));
            System.setProperty("namenode", m_conf.get("fs.default.name"));
            System.setProperty("junit.hadoop.conf", conf_dir.getPath());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void shutdownMiniMrClusters() {
        if (m_mr != null)
            m_mr.shutdown();
        m_mr = null;
    }
}
