package org.triggers;

import org.apache.zookeeper.KeeperException;

import java.net.UnknownHostException;

public interface ServiceCoordinator {
    void leaderElectionCoordinator() throws InterruptedException, KeeperException;
    void workerCoordinator() throws UnknownHostException, InterruptedException, KeeperException;
}
