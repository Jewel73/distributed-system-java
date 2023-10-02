package org.triggers;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ServiceRegistry implements Watcher {
    public static final String REGISTRY_NODE = "/service-registry";
    private ZooKeeper zooKeeper;
    private String currentNode = "";
    private List<String> allServiceAddress;

    ServiceRegistry(ZooKeeper zooKeeper) throws InterruptedException, KeeperException {
        this.zooKeeper = zooKeeper;
        createServiceRegistry();
    }



    public void createServiceRegistry() throws InterruptedException, KeeperException {
        Stat stat = zooKeeper.exists(REGISTRY_NODE, false);
        if (stat == null){
            zooKeeper.create(REGISTRY_NODE, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }



    public void registerInCluster(String address) throws InterruptedException, KeeperException {
        String fullPath = zooKeeper.create(REGISTRY_NODE+"/r_", address.getBytes() ,ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        this.currentNode = fullPath.replace(REGISTRY_NODE+"/", "");
    }



    public void unRegisterFromCluster() throws InterruptedException, KeeperException {

        if(!currentNode.isEmpty() && zooKeeper.exists(REGISTRY_NODE+"/"+currentNode, false) != null)
            zooKeeper.delete(REGISTRY_NODE+"/"+currentNode, -1);
    }


    public void updateAddress() throws InterruptedException, KeeperException {
        List<String> children = zooKeeper.getChildren(REGISTRY_NODE, this);
        List<String> address = new ArrayList<>(children.size());

        for (var child : children){
            String fullPath = REGISTRY_NODE+"/"+child;
            Stat stat = zooKeeper.exists(fullPath, false);
            if (stat == null) continue;

            byte[] data =  zooKeeper.getData(fullPath, false, stat);
            String dataAddress = new String(data);
            address.add(dataAddress);
        }
        this.allServiceAddress = Collections.unmodifiableList(address);
        System.out.println("registered service address: "+ address);
    }



    @Override
    public void process(WatchedEvent watchedEvent) {
        try {
            updateAddress();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        }
    }
}
