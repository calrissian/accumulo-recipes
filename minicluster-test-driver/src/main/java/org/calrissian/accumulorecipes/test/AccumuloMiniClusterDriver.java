package org.calrissian.accumulorecipes.test;

import com.google.common.io.Files;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.commons.io.FileUtils;
import org.junit.rules.ExternalResource;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.*;

import static com.google.common.base.Objects.firstNonNull;

public class AccumuloMiniClusterDriver extends ExternalResource implements Closeable {

    public static String INSTANCE_NAME = firstNonNull(System.getProperty("accumulo.minicluster.instance.name"),"test-instance");
    public static String ROOT_PASSWORD = firstNonNull(System.getProperty("accumulo.minicluster.root.password"),"secret");
    public static int ZOOKEEPER_PORT = Integer.parseInt(firstNonNull(System.getProperty("accumulo.minicluster.zookeeper.port"),findFreePort()+""));
    private MiniAccumuloCluster miniAccumuloCluster;
    private final MiniAccumuloConfig miniAccumuloConfig;
    private final File tempDir = Files.createTempDir();
    private ClientConfiguration clientConfiguration;
    private Instance instance;
    private Connector connector;

    public AccumuloMiniClusterDriver() {
        miniAccumuloConfig = new MiniAccumuloConfig(tempDir,ROOT_PASSWORD);
        miniAccumuloConfig.setZooKeeperPort(ZOOKEEPER_PORT);
        miniAccumuloConfig.setInstanceName(INSTANCE_NAME);
    }

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
                miniAccumuloCluster = new MiniAccumuloCluster(miniAccumuloConfig);
                miniAccumuloCluster.start();
                initInstanceAndConnector(null);
            } catch (Throwable e1) {
                throw new IOException(e1);
            }
        }
    }

    private void initInstanceAndConnector(Integer zkTimeout) throws AccumuloException, AccumuloSecurityException {
        clientConfiguration = new ClientConfiguration()
                .withInstance(miniAccumuloConfig.getInstanceName())
                .withZkHosts("localhost:" + miniAccumuloConfig.getZooKeeperPort());

        if (zkTimeout!=null && zkTimeout!=-1) {
            ExecutorService service = Executors.newSingleThreadExecutor();
            Future<ZooKeeperInstance> future = service.submit(new Callable<ZooKeeperInstance>() {
                @Override
                public ZooKeeperInstance call() throws Exception {
                   return new ZooKeeperInstance(clientConfiguration);
                }
            });
            try {
                instance = future.get(300, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
               throw new AccumuloException(e);
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
        if (miniAccumuloCluster != null) {
            try {
                miniAccumuloCluster.stop();
            } catch (InterruptedException e) {
                //noop
            }
        }

        if (tempDir!=null) {
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
