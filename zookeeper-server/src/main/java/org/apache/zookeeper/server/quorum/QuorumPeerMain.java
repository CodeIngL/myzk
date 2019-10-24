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
package org.apache.zookeeper.server.quorum;

import java.io.File;
import java.io.IOException;

import javax.management.JMException;
import javax.security.sasl.SaslException;

import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.jmx.ManagedUtil;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.DatadirCleanupManager;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;

/**
 *
 * <h2>Configuration file</h2>
 *
 * When the main() method of this class is used to start the program, the first
 * argument is used as a path to the config file, which will be used to obtain
 * configuration information. This file is a Properties file, so keys and
 * values are separated by equals (=) and the key/value pairs are separated
 * by new lines. The following is a general summary of keys used in the
 * configuration file. For full details on this see the documentation in
 * docs/index.html
 * <ol>
 * <li>dataDir - The directory where the ZooKeeper data is stored.</li>
 * <li>dataLogDir - The directory where the ZooKeeper transaction log is stored.</li>
 * <li>clientPort - The port used to communicate with clients.</li>
 * <li>tickTime - The duration of a tick in milliseconds. This is the basic
 * unit of time in ZooKeeper.</li>
 * <li>initLimit - The maximum number of ticks that a follower will wait to
 * initially synchronize with a leader.</li>
 * <li>syncLimit - The maximum number of ticks that a follower will wait for a
 * message (including heartbeats) from the leader.</li>
 * <li>server.<i>id</i> - This is the host:port[:port] that the server with the
 * given id will use for the quorum protocol.</li>
 * </ol>
 * In addition to the config file. There is a file in the data directory called
 * "myid" that contains the server id as an ASCII decimal value.
 *
 */
@InterfaceAudience.Public
public class QuorumPeerMain {
    private static final Logger LOG = LoggerFactory.getLogger(QuorumPeerMain.class);

    private static final String USAGE = "Usage: QuorumPeerMain configfile";

    //分布式中的端点
    protected QuorumPeer quorumPeer;

    /**
     * To start the replicated server specify the configuration file name on
     * the command line.
     * @param args path to the configfile
     */
    public static void main(String[] args) {
        QuorumPeerMain main = new QuorumPeerMain();
        try {
            main.initializeAndRun(args);
        } catch (IllegalArgumentException e) {
            LOG.error("Invalid arguments, exiting abnormally", e);
            LOG.info(USAGE);
            System.err.println(USAGE);
            System.exit(2);
        } catch (ConfigException e) {
            LOG.error("Invalid config, exiting abnormally", e);
            System.err.println("Invalid config, exiting abnormally");
            System.exit(2);
        } catch (Exception e) {
            LOG.error("Unexpected exception, exiting abnormally", e);
            System.exit(1);
        }
        LOG.info("Exiting normally");
        System.exit(0);
    }

    protected void initializeAndRun(String[] args)
        throws ConfigException, IOException
    {
        //quorum模式下的配置
        QuorumPeerConfig config = new QuorumPeerConfig();
        if (args.length == 1) {
            config.parse(args[0]);
        }

        // Start and schedule the the purge task
        // 开始并调度清理任务
        DatadirCleanupManager purgeMgr = new DatadirCleanupManager(config
                .getDataDir(), config.getDataLogDir(), config.getSnapRetainCount(), config.getPurgeInterval());
        purgeMgr.start();

        if (args.length == 1 && config.servers.size() > 0) {
            //集群模式
            runFromConfig(config);
        } else {
            LOG.warn("Either no config or no quorum defined in config, running  in standalone mode");
            // there is only server in the quorum -- run as standalone
            // 独立模式
            ZooKeeperServerMain.main(args);
        }
    }

    /**
     * 运行quorum模式
     * @param config
     * @throws IOException
     */
    public void runFromConfig(QuorumPeerConfig config) throws IOException {
      try {
          ManagedUtil.registerLog4jMBeans();
      } catch (JMException e) {
          LOG.warn("Unable to register log4j JMX control", e);
      }
  
      LOG.info("Starting quorum peer");
      try {
          ServerCnxnFactory cnxnFactory = ServerCnxnFactory.createFactory();
          cnxnFactory.configure(config.getClientPortAddress(), config.getMaxClientCnxns());

          //获得端点
          quorumPeer = getQuorumPeer();

          //设置集群的对端
          quorumPeer.setQuorumPeers(config.getServers());
          //设置事务工厂
          quorumPeer.setTxnFactory(new FileTxnSnapLog(new File(config.getDataLogDir()), new File(config.getDataDir())));
          //选举算法
          quorumPeer.setElectionType(config.getElectionAlg());
          //机器id
          quorumPeer.setMyid(config.getServerId());
          //设置时间单元
          quorumPeer.setTickTime(config.getTickTime());
          //初始化限制
          quorumPeer.setInitLimit(config.getInitLimit());
          //同步限制
          quorumPeer.setSyncLimit(config.getSyncLimit());
          //是否在所有ip上监听
          quorumPeer.setQuorumListenOnAllIPs(config.getQuorumListenOnAllIPs());
          //连接工厂
          quorumPeer.setCnxnFactory(cnxnFactory);
          //是否进行验证
          quorumPeer.setQuorumVerifier(config.getQuorumVerifier());
          //设置客户端地址
          quorumPeer.setClientPortAddress(config.getClientPortAddress());
          //设置最小session超时时间
          quorumPeer.setMinSessionTimeout(config.getMinSessionTimeout());
          //设置最大session超时时间
          quorumPeer.setMaxSessionTimeout(config.getMaxSessionTimeout());
          //设置内存数据库
          quorumPeer.setZKDatabase(new ZKDatabase(quorumPeer.getTxnFactory()));
          //设置leaner类型
          quorumPeer.setLearnerType(config.getPeerType());
          //设置是否开启同步
          quorumPeer.setSyncEnabled(config.getSyncEnabled());
          // sets quorum sasl authentication configurations
          // 设置认证相关，如果配置了相关的配置的话
          quorumPeer.setQuorumSaslEnabled(config.quorumEnableSasl);
          if(quorumPeer.isQuorumSaslAuthEnabled()){
              //sasl相关
              quorumPeer.setQuorumServerSaslRequired(config.quorumServerRequireSasl);
              //sasl相关
              quorumPeer.setQuorumLearnerSaslRequired(config.quorumLearnerRequireSasl);
              //准则
              quorumPeer.setQuorumServicePrincipal(config.quorumServicePrincipal);
              //设置上下文
              quorumPeer.setQuorumServerLoginContext(config.quorumServerLoginContext);
              //设置上下文
              quorumPeer.setQuorumLearnerLoginContext(config.quorumLearnerLoginContext);
          }

          //设置quorum线程数量
          quorumPeer.setQuorumCnxnThreadsSize(config.quorumCnxnThreadsSize);
          //初始化
          quorumPeer.initialize();

          //开始
          quorumPeer.start();
          //等待结束
          quorumPeer.join();
      } catch (InterruptedException e) {
          // warn, but generally this is ok
          LOG.warn("Quorum Peer interrupted", e);
      }
    }

    // @VisibleForTesting
    protected QuorumPeer getQuorumPeer() throws SaslException {
        return new QuorumPeer();
    }
}
