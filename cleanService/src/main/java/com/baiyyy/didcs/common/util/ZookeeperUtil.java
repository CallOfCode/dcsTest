package com.baiyyy.didcs.common.util;

import com.baiyyy.didcs.common.listener.SpringPropertyListener;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * zookeeper工具类
 *
 * @author 逄林
 */
public class ZookeeperUtil {

    /**
     * 获取Client
     * @return
     */
    public static CuratorFramework getClient(){
        CuratorFramework client = null;
        try{
            client = CuratorFrameworkFactory.builder()
                    .connectString(SpringPropertyListener.getPropertyValue("${zookeeper.connectString}"))
                    .retryPolicy(new ExponentialBackoffRetry(Integer.valueOf(SpringPropertyListener.getPropertyValue("${zookeeper.baseSleepTimeMs}")),Integer.valueOf(SpringPropertyListener.getPropertyValue("${zookeeper.maxRetries}"))))
                    .sessionTimeoutMs(Integer.valueOf(SpringPropertyListener.getPropertyValue("${zookeeper.sessionTimeoutMs}")))
                    .connectionTimeoutMs(Integer.valueOf(SpringPropertyListener.getPropertyValue("${zookeeper.connectionTimeoutMs}")))
                    .build();
            client.start();
        }catch(Exception e){
            e.printStackTrace();
        }
        return client;
    }

}
