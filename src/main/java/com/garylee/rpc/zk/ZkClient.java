package com.garylee.rpc.zk;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;


/**
 * Created by GaryLee on 2019-08-28 21:34.
 */
public class ZkClient {
    //日志
    private static final Logger log = LoggerFactory.getLogger(ZkClient.class);
    //服务器
    private String zkServer;
    //服务器结点
    private String serverPath;
    //超时时间
    private int timeout;
    //zookeeper实例
    private ZooKeeper zk;
    //zookeeper监听器
    private Watcher watcher;

    public ZkClient(){super();}

    public ZkClient(String zkServer, String serverPath) {
        this(zkServer,serverPath,20000);
    }

    public ZkClient(String zkServer, String serverPath, int timeout) {
        this.zkServer = zkServer;
        this.serverPath = serverPath;
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
                if(watchedEvent.getType() == Event.EventType.NodeChildrenChanged && serverPath.equals(watchedEvent.getPath())){
                    updateSerrverList();
                }
            }
        };
        getZk();
    }
    /**
     * 更新结点
     */
    private void updateSerrverList(){
        try {
            List<String> servers = zk.getChildren(serverPath,watcher);
            List<String> server = new ArrayList<>();
            //遍历所有节点
            for (String s:server){
                byte[] data = zk.getData(serverPath+"/"+s,false,null);
                server.add(new String(data,"utf-8"));
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    /**
     * 获取zookeeper
     */
    private void getZk(){
        try {
            zk = new ZooKeeper(zkServer,timeout,watcher);
            //查看服务器根节点是否存在
            Stat stat = zk.exists(serverPath,watcher);
            if(stat == null){
                throw new RuntimeException("zk has not started!");
            }else {
                //获取所有服务器
                updateSerrverList();
            }
        }catch (Exception e){
            throw new RuntimeException(e);
        }
    }
}
