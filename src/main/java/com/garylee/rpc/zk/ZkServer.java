package com.garylee.rpc.zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by GaryLee on 2019-08-28 15:17.
 */
public class ZkServer {
    //日志
    private static final Logger log = LoggerFactory.getLogger(ZkServer.class);
    //服务器
    private String zkServer;
    //服务器结点
    private String serverPath;
    //服务器host:port
    private String monitorServer;
    //超时时间
    private int timeout;
    //zookeeper实例
    private ZooKeeper zk;
    //zookeeper监听器
    private Watcher watcher;

    public ZkServer(String zkServer, String serverPath, String monitorServer) {
         this(zkServer,serverPath,monitorServer,20000);
    }

    public ZkServer(String zkServer, String serverPath, String monitorServer, int timeout) {
        this.zkServer = zkServer;
        this.serverPath = serverPath;
        this.monitorServer = monitorServer;
        this.timeout = timeout;

        init();
    }

    /**
     * 加载配置
     */
    private void init(){
        watcher = new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                log.debug(watchedEvent.getPath() +":"+watchedEvent.getType().name());
            }
        };

        try {
            zk = new ZooKeeper(zkServer,timeout,watcher);
            //查看服务器根节点是否存在
            Stat stat = zk.exists(serverPath,watcher);
            if(stat == null){
                zk.create(serverPath,"rpcServer".getBytes("utf-8"), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                log.debug("create znode:" + serverPath);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * zookeeper中注册服务器
     */
    public void register(){
        try {
            zk.create(serverPath+"/rpcServer",monitorServer.getBytes("utf-8"), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT_SEQUENTIAL);
        }catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    /**
     * 获取监听地址
     */
    public String getMonitorServer(){
        return monitorServer;
    }

}
