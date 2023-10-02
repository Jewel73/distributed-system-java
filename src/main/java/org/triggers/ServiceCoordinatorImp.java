package org.triggers;

import org.apache.zookeeper.KeeperException;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class ServiceCoordinatorImp implements ServiceCoordinator{

    private Integer port;
    private ServiceRegistry serviceRegistry;

    ServiceCoordinatorImp(Integer port, ServiceRegistry serviceRegistry){
        this.port = port;
        this.serviceRegistry = serviceRegistry;
    }

    @Override
    public void leaderElectionCoordinator() throws InterruptedException, KeeperException {
        serviceRegistry.unRegisterFromCluster();
        serviceRegistry.updateAddress();
    }

    @Override
    public void workerCoordinator() throws UnknownHostException, InterruptedException, KeeperException {
        String address = String.format("https://%s:%d", InetAddress.getLocalHost().getHostAddress(), port);
        serviceRegistry.registerInCluster(address);
    }
}
