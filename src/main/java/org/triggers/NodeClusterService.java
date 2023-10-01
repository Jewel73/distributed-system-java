package org.triggers;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;


import java.io.IOException;
import java.util.Collections;
import java.util.List;


public class NodeClusterService implements Watcher {
    private static final String PARENT_NODE = "/election";
    private static final String SERVER_LOCATION = "localhost:2181";
    private static final Integer SESSION_TIMEOUT = 3000;
    private ZooKeeper zooKeeper;
    private String currentNode;

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        NodeClusterService cluster = new NodeClusterService();
        cluster.connectZookeper();
        cluster.candidateNodeCreate();
        cluster.reElectLeader();
        cluster.run();
        System.out.println("disconnecting from zookeeper server");
        cluster.close();

    }

    public void connectZookeper() throws IOException {
        zooKeeper = new ZooKeeper(SERVER_LOCATION, SESSION_TIMEOUT, this);
    }

    public void candidateNodeCreate() throws InterruptedException, KeeperException {
        var nodePrefix = PARENT_NODE + "/c_";
        var nodeFullPath = zooKeeper.create(nodePrefix, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        this.currentNode = nodeFullPath.replace(PARENT_NODE+"/", "");
    }

    public void reElectLeader() throws InterruptedException, KeeperException {
        Stat predecessorStat = null ;
        String predecessorName = "" ;
        Stat stat = zooKeeper.exists(PARENT_NODE, false);
        if(stat == null) return;

        List<String> chlidrens =  zooKeeper.getChildren(PARENT_NODE, false);
        Collections.sort(chlidrens);

        String leaderNode = chlidrens.get(0);
        if (leaderNode.equals(currentNode)){
            System.out.println("I am the leader Z node : "+currentNode);
            return;
        }else {
            System.out.println("I am not the leader");
            int predecessorIndex = Collections.binarySearch(chlidrens, currentNode) - 1;
            predecessorName = chlidrens.get(predecessorIndex);
            zooKeeper.exists(PARENT_NODE+ "/"+ predecessorName, this);
        }
        System.out.println("I am watching Z node : "+ predecessorName);
        System.out.println();
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

            case NodeDeleted:
                try {
                    reElectLeader();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } catch (KeeperException e) {
                    throw new RuntimeException(e);
                }

        }
    }
}
