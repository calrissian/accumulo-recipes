/*
 * Copyright (C) 2016 The Calrissian Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.calrissian.accumulorecipes.test;

import com.google.common.io.Files;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.*;

import static com.google.common.base.Objects.firstNonNull;

public class AccumuloMiniClusterDriver extends ExternalResource implements Closeable {

    public static final Logger log = LoggerFactory.getLogger(AccumuloMiniClusterDriver.class);

    public static String INSTANCE_NAME = firstNonNull(System.getProperty("accumulo.minicluster.instance.name"),"test-instance");
    public static String ROOT_PASSWORD = firstNonNull(System.getProperty("accumulo.minicluster.root.password"),"secret");
    public static int ZOOKEEPER_PORT = Integer.parseInt(firstNonNull(System.getProperty("accumulo.minicluster.zookeeper.port"),findFreePort()+""));
    private MiniAccumuloCluster miniAccumuloCluster;
    private MiniAccumuloConfig miniAccumuloConfig;
    private File tempDir;
    private ClientConfiguration clientConfiguration;
    private Instance instance;
    private Connector connector;

    public AccumuloMiniClusterDriver() {}

    @Override
    protected void before() throws Throwable {
       start();
    }

    @Override
    protected void after() {
        try {
            close();
        } catch (IOException e) {
            //noop
        }
    }

    public void deleteAllTables() throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        for (String table : connector.tableOperations().list()) {
            if (table.startsWith("accumulo.")) {
                continue;
            }
            connector.tableOperations().delete(table);
        }
    }

    public void start() throws IOException {
        try {
            initInstanceAndConnector(300);
        } catch (Throwable e) {
            try {
                initConfig();
                miniAccumuloCluster = new MiniAccumuloCluster(miniAccumuloConfig);
                miniAccumuloCluster.start();
                log.info("started minicluster");
                initInstanceAndConnector(null);
            } catch (Throwable e1) {
                throw new IOException(e1);
            }
        }
    }

    private Boolean isZookeeperRunning(String host, int timeout) {
        final CountDownLatch connectedSignal = new CountDownLatch(1);
        try {
            new ZooKeeper(host, timeout,
                        new Watcher() {
                            public void process(WatchedEvent event) {
                                if (event.getState() == Event.KeeperState.SyncConnected) {
                                    connectedSignal.countDown();
                                }
                            }
                        });
            return connectedSignal.await(timeout,TimeUnit.MILLISECONDS);
        } catch (Throwable e) {
            return Boolean.FALSE;
        }
    }

    private void initConfig() {
        tempDir = Files.createTempDir();
        miniAccumuloConfig = new MiniAccumuloConfig(tempDir,ROOT_PASSWORD);
        miniAccumuloConfig.setZooKeeperPort(ZOOKEEPER_PORT);
        miniAccumuloConfig.setInstanceName(INSTANCE_NAME);
        log.info("attempting to start minicluster in " + tempDir.getAbsolutePath());
    }

    private void initInstanceAndConnector(final Integer zkTimeout) throws AccumuloException, AccumuloSecurityException {
        clientConfiguration = new ClientConfiguration()
                .withInstance(INSTANCE_NAME)
                .withZkHosts("localhost:" + ZOOKEEPER_PORT);

        if (zkTimeout!=null && zkTimeout!=-1) {
            if (isZookeeperRunning("localhost:"+ZOOKEEPER_PORT,zkTimeout)) {
                instance = new ZooKeeperInstance(clientConfiguration);
            }
        } else {
            instance = new ZooKeeperInstance(clientConfiguration);
        }

        connector = instance.getConnector("root",new PasswordToken(ROOT_PASSWORD));
    }

    public ClientConfiguration getClientConfiguration() {
        return clientConfiguration;
    }

    public Instance getInstance() {
        return instance;
    }

    public Connector getConnector() {
        return connector;
    }

    private static int findFreePort() {
        ServerSocket socket = null;
        try {
            socket = new ServerSocket(0);
            socket.setReuseAddress(true);
            int port = socket.getLocalPort();
            try {
                socket.close();
            } catch (IOException e) {
                // Ignore IOException on close()
            }
            return port;
        } catch (IOException e) {
        } finally {
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException e) {
                }
            }
        }
        throw new IllegalStateException("Could not find a free TCP/IP port for zookeeper");
    }


    @Override
    public void close() throws IOException {
        log.info("attempting to close");
        if (miniAccumuloCluster != null) {
            try {
                log.info("miniAccumuloCluster found attempting to stop it");
                miniAccumuloCluster.stop();
                log.info("stopped miniAccumuloCluster");
            } catch (InterruptedException e) {
                //noop
            }
        }

        if (tempDir!=null) {
            log.info("deleting temp dir: "+ tempDir.getAbsolutePath());
            FileUtils.deleteQuietly(tempDir);
        }
    }

    public String getRootPassword() {
        return ROOT_PASSWORD;
    }

    public void setRootAuths(Authorizations auths) throws AccumuloSecurityException, AccumuloException {
        connector.securityOperations().changeUserAuthorizations("root",auths);
    }

    public String getZooKeepers() {
        if (miniAccumuloCluster==null) {
            return "localhost:"+ZOOKEEPER_PORT;
        }
        return miniAccumuloCluster.getZooKeepers();
    }

    public String getInstanceName() {
        if (miniAccumuloCluster==null) {
            return INSTANCE_NAME;
        }
        return miniAccumuloCluster.getInstanceName();
    }
}
