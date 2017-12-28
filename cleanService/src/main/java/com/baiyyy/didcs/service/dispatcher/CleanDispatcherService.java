package com.baiyyy.didcs.service.dispatcher;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.baiyyy.didcs.common.constant.FlowConstant;
import com.baiyyy.didcs.common.constant.LogConstant;
import com.baiyyy.didcs.common.constant.ZooCacheConstant;
import com.baiyyy.didcs.common.util.MapUtil;
import com.baiyyy.didcs.common.util.SpringContextUtil;
import com.baiyyy.didcs.common.util.ZookeeperUtil;
import com.baiyyy.didcs.common.vo.JsonResult;
import com.baiyyy.didcs.dao.dispatcher.CleanDispatcherMapper;
import com.baiyyy.didcs.interfaces.invoker.IStageServiceInvoker;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.CloseableUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.*;

import static net.logstash.logback.marker.Markers.*;
/**
 * 清洗调度服务
 *
 * @author 逄林
 */
@Service
public class CleanDispatcherService {
    private Logger logger = LoggerFactory.getLogger(CleanDispatcherService.class);

    @SuppressWarnings("SpringJavaAutowiringInspection")
    @Autowired
    private CleanDispatcherMapper cleanDispatcherMapper;
    @Autowired
    private ZooConfCacheService zooConfCacheService;

    /**
     * 自动调度下一清洗服务
     * @param batchId
     * @param userId
     * @return
     */
    public JsonResult<String> dispatch(String batchId, String userId) {
        JsonResult<String> r = new JsonResult<>();
        r.setResult(true);
        r.setMsg("调度成功");
        CuratorFramework client = ZookeeperUtil.getClient();
        String rootPath = ZooCacheConstant.BATCH_PATH + "/" + batchId;
        try {
            if (null != client.checkExists().forPath(rootPath)) {
                JSONObject root = JSONObject.parseObject(new String(client.getData().forPath(rootPath)));
                int status = root.getInteger("status");
                JSONArray stages = root.getJSONArray("stages");
                if (FlowConstant.CLEAN_STATUS_NOT_BEGIN == status) {
                    //0.判断是否有配置清洗流程阶段
                    if (stages.size() > 0) {
                        logger.info(append(LogConstant.LGS_FIELD_TAGS,LogConstant.LGS_TAGS_DISPATCH).and(append("batchId",batchId)),LogConstant.LGS_DISPATCH_SUCCMSG_SVBEGIN);
                        //1.修改数据库状态
                        cleanDispatcherMapper.updateBatchStatus(batchId, FlowConstant.CLEAN_STATUS_CLEANING + "");
                        //2.开启清洗流程
                        root.put("status", FlowConstant.CLEAN_STATUS_CLEANING);
                        client.setData().forPath(rootPath, root.toJSONString().getBytes());
                        //3.线程调用下一个可执行清洗的节点
                        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder()
                                .setNameFormat("批次:"+batchId+":调度线程-%d").build();
                        ExecutorService service = new ThreadPoolExecutor(1, 1,
                                0L, TimeUnit.MILLISECONDS,
                                new LinkedBlockingQueue<Runnable>(),namedThreadFactory);
                        service.submit(new Runnable() {
                            @Override
                            public void run() {
                                invokeNextService(batchId, stages.toArray(new String[]{}), userId);
                            }
                        });
                        service.shutdown();
                    } else {
                        finishClean(batchId, client);
                    }
                } else if (FlowConstant.CLEAN_STATUS_CLEANING == status) {
                    //调用下一个可执行清洗的节点
                    if (stages.size() > 0) {
                        ExecutorService service = Executors.newFixedThreadPool(1);
                        service.submit(new Runnable() {
                            @Override
                            public void run() {
                                invokeNextService(batchId, stages.toArray(new String[]{}), userId);
                            }
                        });
                        service.shutdown();
                    }
                } else if (FlowConstant.CLEAN_STATUS_STOP == status) {
                    //如果状态为中止，则停止清洗
                    finishClean(batchId, client);
                }
            } else {
                finishClean(batchId, client);
                r.setResult(false);
                r.setMsg("清洗配置不存在");
            }
        } catch (Exception e) {
            logger.error(append(LogConstant.LGS_FIELD_TAGS,LogConstant.LGS_TAGS_DISPATCH).and(append("batchId",batchId)),LogConstant.LGS_DISPATCH_ERRORMSG_NEXT,e );
        } finally {
            CloseableUtils.closeQuietly(client);
        }

        return r;
    }

    /**
     * 停止当前服务并调度下一服务
     * @param batchId
     * @param userId
     * @return
     */
    public JsonResult<String> stopAndDispatch(String batchId, String userId){
        JsonResult<String> r = new JsonResult<>();
        r.setResult(true);
        r.setMsg("调度成功");
        CuratorFramework client = ZookeeperUtil.getClient();
        String rootPath = ZooCacheConstant.BATCH_PATH + "/" + batchId;

        try {
            if (null != client.checkExists().forPath(rootPath)) {
                JSONObject root = JSONObject.parseObject(new String(client.getData().forPath(rootPath)));
                int status = root.getInteger("status");
                JSONArray stages = root.getJSONArray("stages");
                //调用下一个可执行清洗的节点
                if (stages.size() > 0) {
                    stopAndInvokeNextService(client, batchId, stages.toArray(new String[]{}), userId);
                }else{
                    finishClean(batchId, client);
                    r.setResult(false);
                    r.setMsg("清洗配置不存在");
                }
            } else {
                finishClean(batchId, client);
                r.setResult(false);
                r.setMsg("清洗配置不存在");
            }
        } catch (Exception e) {
            logger.error(append(LogConstant.LGS_FIELD_TAGS,LogConstant.LGS_TAGS_DISPATCH).and(append("batchId",batchId)),LogConstant.LGS_DISPATCH_ERRORMSG_STOP_NEXT,e );
        } finally {
            CloseableUtils.closeQuietly(client);
        }
        return r;
    }

    /**
     * 检查并初始化清洗配置
     * @param batchId
     * @return Boolean 返回结果为true则表示配置检查成功，否则失败
     */
    public Boolean checkAndInitConf(String batchId){
        Boolean r = null;
        CuratorFramework client = ZookeeperUtil.getClient();
        String basePath = ZooCacheConstant.BATCH_PATH + "/" + batchId;
        try{
            if(null==client.checkExists().forPath(basePath)){
                //1.如果配置不存在，则新建配置
                JsonResult<String> result = zooConfCacheService.initConfByBatchId(batchId);
                r = result.getResult();
            }else{
                JSONObject conf = JSONObject.parseObject(new String(client.getData().forPath(basePath)));
                int status = conf.getInteger("status");
                if(status==FlowConstant.CLEAN_STATUS_NOT_BEGIN){
                    String version = conf.getString("version");
                    String dBVersion = zooConfCacheService.getVersionByBatchId(batchId);
                    //如果版本不一致,则删除原配置并更新配置
                    if(!version.equals(dBVersion)){
                        client.delete().deletingChildrenIfNeeded().forPath(basePath);
                        JsonResult<String> result = zooConfCacheService.initConfByBatchId(batchId);
                        r = result.getResult();
                    }else{
                        r = true;
                    }
                }else{
                    r = false;
                }
            }

        }catch(Exception e){
            logger.error(append(LogConstant.LGS_FIELD_TAGS,LogConstant.LGS_TAGS_DISPATCH).and(append("batchId",batchId)),LogConstant.LGS_DISPATCH_ERRORMSG_INIT,e );
            r = false;
        }finally{
            CloseableUtils.closeQuietly(client);
        }
        return r;
    }

    /**
     * 获取下一服务并执行
     * @param batchId
     * @param stages
     * @param userId
     * @return
     * @throws Exception
     */
    private JsonResult invokeNextService(String batchId, String[] stages, String userId) {
        CuratorFramework client = ZookeeperUtil.getClient();
        JsonResult r = new JsonResult();
        String basePath = ZooCacheConstant.BATCH_PATH + "/" + batchId;
        Boolean hasRuning = false;
        JSONObject serviceNode = null;
        JSONObject stageNode = null;
        String serviceStageId = "";
        for (String stage : stages) {
            if (hasRuning) {
                r.setResult(false);
                r.setMsg("有服务正在运行，不能进行流程调度");
                break;
            }
            if (null != serviceNode) {
                r.setResult(true);
                break;
            }
            String stagePath = basePath + "/" + stage;
            JSONObject stageObj = null;
            try {
                stageObj = JSONObject.parseObject(new String(client.getData().forPath(stagePath)));
            } catch (Exception e) {
                e.printStackTrace();
            }
            JSONArray services = stageObj.getJSONArray("services");
            if(null!=services){
                for (int i = 0; i < services.size(); i++) {
                    JSONObject svObj = services.getJSONObject(i);
                    int status = svObj.getInteger("status");
                    if (FlowConstant.SERVICE_STATUS_NOT_BEGIN == status) {
                        serviceNode = svObj;
                        stageNode = stageObj;
                        serviceStageId = stage;
                        break;
                    } else if (FlowConstant.SERVICE_STATUS_CLEANING == status) {
                        hasRuning = true;
                        break;
                    }
                }
            }
        }

        if (null != serviceNode) {
            //获取到了下个将要执行的服务,调用服务
            try {
                //更新节点状态
                serviceNode.put("status", FlowConstant.SERVICE_STATUS_CLEANING);
                serviceNode.put("starttime", DateTime.now().toString("yyyy-MM-dd HH:mm:ss"));
                client.setData().forPath(basePath + "/" + serviceStageId, stageNode.toJSONString().getBytes());

                IStageServiceInvoker invoker = SpringContextUtil.getBean(serviceNode.getString("svname"));

                Map idMap = cleanDispatcherMapper.getSchemaIdAndTaskIdByBatchId(batchId);
                Boolean invokeResult = invoker.invokerService(MapUtil.getCasedString(idMap.get("schema_id")), MapUtil.getCasedString(idMap.get("task_id")), batchId, serviceNode.toJSONString(), userId);
                if(!invokeResult){
                    finishClean(batchId,client);
                }
            } catch (Exception e) {
                logger.error(append(LogConstant.LGS_FIELD_TAGS,LogConstant.LGS_TAGS_DISPATCH).and(append("batchId",batchId)).and(append("stage",serviceStageId)),LogConstant.LGS_DISPATCH_ERRORMSG_SVINVOKE,e );
                //调用服务出错则终止流程
                finishClean(batchId, client);
                r.setResult(false);
                r.setMsg("有服务正在运行，不能进行流程调度");
            }
        } else {
            //获取不到服务，则流程完成
            finishClean(batchId, client);
            r.setResult(true);
            r.setMsg("流程调度完成");
        }
        CloseableUtils.closeQuietly(client);
        return r;
    }

    /**
     * 停止当前服务，并自动启动下一服务
     * @param client
     * @param batchId
     * @param stages
     * @param userId
     * @return
     * @throws Exception
     */
    private JsonResult stopAndInvokeNextService(CuratorFramework client, String batchId, String[] stages, String userId){
        JsonResult r = new JsonResult();
        String basePath = ZooCacheConstant.BATCH_PATH + "/" + batchId;
        JSONObject serviceNode = null;
        JSONObject stageNode = null;
        String serviceStageId = "";
        for (String stage : stages) {
            if (null != serviceNode) {
                r.setResult(true);
                break;
            }
            String stagePath = basePath + "/" + stage;
            JSONObject stageObj = null;
            try {
                stageObj = JSONObject.parseObject(new String(client.getData().forPath(stagePath)));
            } catch (Exception e) {
                e.printStackTrace();
            }
            JSONArray services = stageObj.getJSONArray("services");
            for (int i = 0; i < services.size(); i++) {
                JSONObject svObj = services.getJSONObject(i);
                int status = svObj.getInteger("status");
                if(FlowConstant.SERVICE_STATUS_CLEANING == status){
                    //完成当前服务
                    svObj.put("status", FlowConstant.SERVICE_STATUS_END);
                    svObj.put("endtime", DateTime.now().toString("yyyy-MM-dd HH:mm:ss"));
                    try {
                        client.setData().forPath(stagePath, stageObj.toJSONString().getBytes());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }else if (FlowConstant.SERVICE_STATUS_NOT_BEGIN == status) {
                    serviceNode = svObj;
                    stageNode = stageObj;
                    serviceStageId = stage;
                    break;
                }
            }
        }

        if (null != serviceNode) {
            //获取到了下个将要执行的服务,调用服务
            try {
                //更新节点状态
                serviceNode.put("status", FlowConstant.SERVICE_STATUS_CLEANING);
                serviceNode.put("starttime", DateTime.now().toString("yyyy-MM-dd HH:mm:ss"));
                client.setData().forPath(basePath + "/" + serviceStageId, stageNode.toJSONString().getBytes());

                IStageServiceInvoker invoker = SpringContextUtil.getBean(serviceNode.getString("svname"));

                Map idMap = cleanDispatcherMapper.getSchemaIdAndTaskIdByBatchId(batchId);
                invoker.invokerService(MapUtil.getCasedString(idMap.get("schema_id")), MapUtil.getCasedString(idMap.get("task_id")), batchId, serviceNode.toJSONString(), userId);
            } catch (Exception e) {
                //调用服务出错则终止流程
                logger.error(append(LogConstant.LGS_FIELD_TAGS,LogConstant.LGS_TAGS_DISPATCH).and(append("batchId",batchId)).and(append("stage",serviceStageId)),LogConstant.LGS_DISPATCH_ERRORMSG_SVINVOKE,e );
                finishClean(batchId, client);
                r.setResult(false);
                r.setMsg("有服务正在运行，不能进行流程调度");
            }
        } else {
            //获取不到服务，则流程完成
            finishClean(batchId, client);
            r.setResult(true);
            r.setMsg("流程调度完成");
        }
        return r;
    }

    private void finishClean(String batchId, CuratorFramework client) {
        //1.删除配置路径
        String rootPath = ZooCacheConstant.BATCH_PATH + "/" + batchId;
        //2.删除锁路径
        String lockPath = ZooCacheConstant.LOCK_PATH + "/" + batchId;
        try {
            if (null != client.checkExists().forPath(rootPath)) {
                client.delete().deletingChildrenIfNeeded().forPath(rootPath);
            }
            if (null != client.checkExists().forPath(lockPath)) {
                client.delete().deletingChildrenIfNeeded().forPath(lockPath);
            }
        } catch (Exception e) {
            logger.error(append(LogConstant.LGS_FIELD_TAGS,LogConstant.LGS_TAGS_DISPATCH),LogConstant.LGS_DISPATCH_ERRORMSG_FINISH,e );
        }
        //2.变更数据库状态
        cleanDispatcherMapper.updateBatchStatus(batchId, FlowConstant.CLEAN_STATUS_NOT_BEGIN + "");
        logger.info(append(LogConstant.LGS_FIELD_TAGS,LogConstant.LGS_TAGS_DISPATCH).and(append("batchId",batchId)),LogConstant.LGS_DISPATCH_SUCCMSG_FINISH );
    }

}
