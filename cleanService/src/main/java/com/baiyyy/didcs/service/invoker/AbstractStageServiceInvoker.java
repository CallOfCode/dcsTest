package com.baiyyy.didcs.service.invoker;

import com.alibaba.fastjson.JSONObject;
import com.baiyyy.didcs.common.constant.LogConstant;
import com.baiyyy.didcs.common.constant.ZooCacheConstant;
import com.baiyyy.didcs.common.listener.SpringPropertyListener;
import com.baiyyy.didcs.common.util.JoddHttpUtil;
import com.baiyyy.didcs.common.util.MapUtil;
import com.baiyyy.didcs.common.util.ZookeeperUtil;
import com.baiyyy.didcs.interfaces.invoker.IStageServiceInvoker;
import com.baiyyy.didcs.service.dispatcher.LockService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.shared.SharedCount;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

import static net.logstash.logback.marker.Markers.append;

/**
 * 阶段服务调用抽象接口
 *
 * @author 逄林
 */
public abstract class AbstractStageServiceInvoker implements IStageServiceInvoker {
    private JSONObject service = null;
    private String batchId = null;
    private String schemaId = null;
    private String taskId = null;
    private String serviceJson = null;
    private String userId = null;
    private String lockPath = null;
    private Map<String,Object> httpParam = null;
    private Integer threadNums = null;

    /**
     * 获取调用器名称
     * @return
     */
    public abstract String getInvokerName();

    /**
     * 获取Logger
     * @return
     */
    public abstract Logger getLogger();

    /**
     * 获取锁服务
     * @return
     */
    public abstract LockService getLockService();

    /**
     * 获取不同线程的起止id
     * @param threads
     * @param param
     * @return
     */
    public abstract List<Map<String ,Object>> getHeadIds(int threads,Map<String,Object> param);

    @Override
    public Boolean invokerService(String schemaId, String taskId, String batchId, String serviceJson, String userId) {
        Boolean result = null;
        parseService(schemaId, taskId, batchId, serviceJson, userId);

        if (createCounter()) {
            if (execThreads()) {
                result = true;
            } else {
                result = false;
            }
        } else {
            result = false;
        }

        return result;
    }

    /**
     * 根据线程数创建计数器
     * @return
     */
    private Boolean createCounter() {
        Boolean r = true;
        if (null != service) {
            CuratorFramework client = ZookeeperUtil.getClient();
            int threads = getThreadNums();
            try {
                if (null == client.checkExists().forPath(lockPath)) {
                    client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(lockPath);
                }
                SharedCount baseCount = new SharedCount(client, lockPath, 0);
                baseCount.start();
                baseCount.setCount(threads);
                baseCount.close();
            } catch (Exception e) {
                r = false;
                getLogger().error(append(LogConstant.LGS_FIELD_TAGS,LogConstant.LGS_TAGS_FLOW).and(append("batchId",batchId)).and(append("lockPath",lockPath)).and(append("threads",threads)),LogConstant.LGS_FLOW_ERRORMSG_COUNTER,e );
            }finally {
                CloseableUtils.closeQuietly(client);
            }
        } else {
            r = false;
        }
        return r;
    }



    /**
     * 执行线程
     * @return
     */
    private Boolean execThreads() {
        String url = SpringPropertyListener.getPropertyValue("${stageService.baseUrl}")+SpringPropertyListener.getPropertyValue("${stageService.serviceInvokerUrl}");
        //调用远程服务
        int threads = getThreadNums();
        Map params = getHttpParam();
        if(params.containsKey("limitIds")&& !MapUtil.isBlank(params.get("limitIds"))){
            //在包含limitIds的情况下，不进行多线程操作
            if(threads>1){
                for(int i=1;i<threads;i++){
                    //减少多余的计数器
                    getLockService().subLockNum(getLockPath(),getBatchId(),getUserId());
                }
            }
            List<Map> paramsList = new ArrayList<>(1);
            paramsList.add(params);
            //启动线程
            startThreadService(1,url,paramsList);
        }else{
            if(threads==1){
                List<Map> paramsList = new ArrayList<>(1);
                paramsList.add(params);
                //启动线程
                startThreadService(1,url,paramsList);
            }else{
                List<Map<String ,Object>> idMapList = getHeadIds(threads,params);
                List<Map> paramsList = new ArrayList<>();
                for(Map idMap:idMapList){
                    String maxId = MapUtil.getCasedString(idMap.get("maxId"));
                    String minId = MapUtil.getCasedString(idMap.get("minId"));
                    if(maxId.equals(minId)&&"0".equals(maxId)){
                        getLockService().subLockNum(getLockPath(),getBatchId(),getUserId());
                    }else{
                        Map paramMap = new HashMap();
                        paramMap.putAll(params);
                        paramMap.put("maxId",maxId);
                        paramMap.put("minId",minId);
                        paramsList.add(paramMap);
                    }
                }
                //启动线程
                startThreadService(paramsList.size(),url,paramsList);
            }
        }
        return true;
    }

    private void startThreadService(int threads,String url,List<Map> paramsList){
        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder()
                .setNameFormat("批次:"+getBatchId()+":"+getInvokerName()+"流程-%d").build();
        ExecutorService service = new ThreadPoolExecutor(threads, threads,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(),namedThreadFactory);

        for(Map params:paramsList){
            service.submit(new Runnable() {
                @Override
                public void run() {
                    try{
                        JoddHttpUtil.doPost(url,params);
                    }catch(Exception e){
                        getLogger().error(append(LogConstant.LGS_FIELD_TAGS,LogConstant.LGS_TAGS_FLOW).and(append("serviceUrl",url)),LogConstant.LGS_FLOW_ERRORMSG_THREAD,e );
                        //如果调用失败则计数器-1
                        getLockService().subLockNum(getLockPath(),getBatchId(),getUserId());
                    }
                }
            });
        }
        service.shutdown();
    }

    public JSONObject getService() {
        return service;
    }

    public String getBatchId() {
        return batchId;
    }

    public String getSchemaId() {
        return schemaId;
    }

    public String getTaskId() {
        return taskId;
    }

    public String getServiceJson() {
        return serviceJson;
    }

    public String getUserId() {
        return userId;
    }

    public String getLockPath() {
        return lockPath;
    }

    public Map<String,Object> getHttpParam(){
        return httpParam;
    }

    /**
     * 获取线程数量
     * @return
     */
    public Integer getThreadNums(){
        if(null==this.threadNums){
            this.threadNums = initThreadNums();
        }
        return this.threadNums;
    }

    /**
     * 初始化线程数量
     * @return
     */
    public abstract Integer initThreadNums();

    /**
     * 参数转换
     * @param schemaId
     * @param taskId
     * @param batchId
     * @param serviceJson
     * @param userId
     */
    private void parseService(String schemaId, String taskId, String batchId, String serviceJson, String userId) {
        this.schemaId = schemaId;
        this.taskId = taskId;
        this.batchId = batchId;
        this.serviceJson = serviceJson;
        this.userId = userId;
        this.lockPath = ZooCacheConstant.LOCK_PATH + "/" + batchId + "/" + new Random().nextInt(10);

        if (!(null == serviceJson || "".equals(serviceJson.trim()))) {
            this.service = JSONObject.parseObject(serviceJson);
        }

        if(!(service.containsKey("threadable") && "1".equals(service.getString("threadable")))){
            //如果参数未设置或设置为不开启多线程，则线程数量默认设置为1
            this.threadNums = 1;
        }

        this.httpParam = new HashMap<>();
        httpParam.put("serviceStr",serviceJson);
        httpParam.put("schemaId",schemaId);
        httpParam.put("taskId",taskId);
        httpParam.put("batchId",batchId);
        httpParam.put("maxId",null);
        httpParam.put("minId",null);
        httpParam.put("limitIds",null);
        httpParam.put("handleSize",null);
        httpParam.put("lockPath",lockPath);
        httpParam.put("userId",userId);
    }
}
