package org.example;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class LeaderSelection implements Watcher {

    private ZooKeeper zooKeeper;
    private  static final String ZOOKEEPER_LOCATION = "localhost:2181";
    private static final String MONITOR_ZNODE = "/test";
    private static final int TIMEOUT=3000;

    public static void main(String[] args) throws IOException, InterruptedException {
        LeaderSelection leaderSelection = new LeaderSelection();
        leaderSelection.connectToZookeeper();
        leaderSelection.run();
        leaderSelection.close();
        System.out.println("Disconnected from Zookeeper, exiting application");
    }

    public void connectToZookeeper() throws IOException {
        this.zooKeeper = new ZooKeeper(ZOOKEEPER_LOCATION, TIMEOUT, this);
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

    public void electedLeader() throws InterruptedException, KeeperException {
        Stat stat =  zooKeeper.exists(MONITOR_ZNODE, this);
        if (stat == null) return;

        byte[] data =  zooKeeper.getData(MONITOR_ZNODE, this, stat);
        List<String> children = zooKeeper.getChildren(MONITOR_ZNODE, this);

        System.out.println("Data from Znode: "+ new String(data) + " children"+ children);

    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        switch (watchedEvent.getType()){
            case None:
                if (watchedEvent.getState() == Event.KeeperState.SyncConnected){
                    System.out.println("Successfully connected to Zookeeper");
                }else {
                    synchronized (zooKeeper){
                        zooKeeper.notifyAll();
                    }
                }
            case NodeCreated:
                System.out.println("Node created event");
                break;

            case NodeDeleted:
                System.out.println("Node has been deleted");
                break;

            case NodeDataChanged:
                System.out.println("Data changed in the node");
                break;

            case NodeChildrenChanged:
                System.out.println("node has changed.");
                break;

        }

        try {
            electedLeader();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        }
    }
}
