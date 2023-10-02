package org.triggers;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;


public class NodeClusterService implements Watcher {
    private static final String PARENT_NODE = "/election";
    private ZooKeeper zooKeeper;
    private String currentNode = "";
    private ServiceCoordinator serviceCoordinator;

    NodeClusterService(ZooKeeper zooKeeper, ServiceCoordinator serviceCoordinator){
        this.zooKeeper = zooKeeper;
        this.serviceCoordinator = serviceCoordinator;
    }


    public void candidateNodeCreate() throws InterruptedException, KeeperException {
        var nodePrefix = PARENT_NODE + "/c_";
        var nodeFullPath = zooKeeper.create(nodePrefix, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        this.currentNode = nodeFullPath.replace(PARENT_NODE+"/", "");
    }

    public void reElectLeader() throws InterruptedException, KeeperException, UnknownHostException {
        Stat predecessorStat = null ;
        String predecessorName = "" ;
        Stat stat = zooKeeper.exists(PARENT_NODE, false);
        if(stat == null) return;

        List<String> chlidrens =  zooKeeper.getChildren(PARENT_NODE, true);
        Collections.sort(chlidrens);

        String leaderNode = chlidrens.get(0);
        if (leaderNode.equals(currentNode)){
            System.out.println("I am the leader Z node : "+currentNode);
            serviceCoordinator.leaderElectionCoordinator();
            return;
        }else {
            System.out.println("I am not the leader");
            int predecessorIndex = Collections.binarySearch(chlidrens, currentNode) - 1;
            predecessorName = chlidrens.get(predecessorIndex);
            zooKeeper.exists(PARENT_NODE+ "/"+ predecessorName, this);
        }
        System.out.println("I am watching Z node : "+ predecessorName);
        System.out.println();
        serviceCoordinator.workerCoordinator();
    }




    @Override
    public void process(WatchedEvent watchedEvent) {
        switch (watchedEvent.getType()){

            case NodeDeleted:
                try {
                    reElectLeader();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } catch (KeeperException | UnknownHostException e) {
                    throw new RuntimeException(e);
                }

        }
    }
}
