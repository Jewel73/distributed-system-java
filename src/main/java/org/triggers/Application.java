package org.triggers;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

public class Application implements Watcher {
    private static final String SERVER_LOCATION = "localhost:2181";
    private static final Integer SESSION_TIMEOUT = 30000;
    private ZooKeeper zooKeeper;
    private static final Integer DEFAULT_PORT = 8011;

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {

        Integer val = args.length == 0? DEFAULT_PORT: Integer.parseInt(args[0]);

        Application application = new Application();
        application.zooKeeper = application.connectZookeper();
        ServiceRegistry serviceRegistry = new ServiceRegistry(application.zooKeeper);
        ServiceCoordinator serviceCoordinator = new ServiceCoordinatorImp(val, serviceRegistry);

        NodeClusterService nodeClusterService = new NodeClusterService(application.zooKeeper, serviceCoordinator);

        nodeClusterService.candidateNodeCreate();
        nodeClusterService.reElectLeader();

        application.run();
        application.close();



    }

    private ZooKeeper connectZookeper() throws IOException, IOException {
        return new ZooKeeper(SERVER_LOCATION, SESSION_TIMEOUT, this);
    }

    public void run() throws InterruptedException {
        synchronized (zooKeeper){
            zooKeeper.wait();
        }
    }

    public void close() throws InterruptedException {
        synchronized (zooKeeper){
            zooKeeper.close();
        }
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        switch (watchedEvent.getType()){
            case None:
                if ((watchedEvent.getState() == Event.KeeperState.SyncConnected)){
                    System.out.println("Successfully connected to the zookeeper");
                }else {
                    synchronized (zooKeeper){
                        System.out.println("disconnected from zookeeper");
                        zooKeeper.notifyAll();
                    }
                }
                break;
        }
    }
}
