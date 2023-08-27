package org.example;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

public class LeaderSelection implements Watcher {

    private ZooKeeper zooKeeper;
    private  static final String ZOOKEEPER_LOCATION = "localhost:2181";
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
        }
    }
}
