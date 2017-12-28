package com.baiyyy.didcs.service.dispatcher;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.baiyyy.didcs.common.constant.ConfConstant;
import com.baiyyy.didcs.common.constant.LogConstant;
import com.baiyyy.didcs.common.constant.ZooCacheConstant;
import com.baiyyy.didcs.common.util.MapUtil;
import com.baiyyy.didcs.common.util.ZookeeperUtil;
import com.baiyyy.didcs.common.vo.JsonResult;
import com.baiyyy.didcs.dao.dispatcher.ZooConfCacheMapper;
import com.sun.org.apache.xpath.internal.operations.Bool;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.CreateMode;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static net.logstash.logback.marker.Markers.append;

/**
 * 与基于zookeeper的配置缓存相关的操作service
 *
 * @author 逄林
 */
@Service
public class ZooConfCacheService {
    Logger logger = LoggerFactory.getLogger(ZooConfCacheService.class);

    @SuppressWarnings("SpringJavaAutowiringInspection")
    @Autowired
    private ZooConfCacheMapper zooConfCacheMapper;

    /**
     * 全部流程缓存初始化
     *
     * @return
     */
    public JsonResult<String> initAllConf() {
        CuratorFramework client = ZookeeperUtil.getClient();
        JsonResult<String> r = new JsonResult<>();
        r.setResult(true);
        r.setMsg("初始化完成");
        try {
            //1.清空所有配置
            removeAllConfFromCache(client);
            //2.全部初始化
            createAllConf(client);
        } catch (Exception e) {
            logger.error(append(LogConstant.LGS_FIELD_TAGS,LogConstant.LGS_TAGS_CONF),LogConstant.LGS_CONF_ERRORMSG_INITALL,e );
            r.setResult(false);
            r.setMsg("初始化缓存出错");
        } finally {
            CloseableUtils.closeQuietly(client);
        }
        return r;
    }

    /**
     * 根据批次ID实例化配置
     *
     * @param batchId
     * @return
     */
    public JsonResult<String> initConfByBatchId(String batchId) {
        CuratorFramework client = ZookeeperUtil.getClient();
        JsonResult<String> r = new JsonResult<>();
        r.setResult(true);
        r.setMsg("初始化完成");
        try {
            //1.检查缓存
            String rooPath = checkAndInitConfCacheByBatchId(batchId, client);
            //2.加载实例化缓存
            makeBatchConfInstance(batchId, rooPath, client);
        } catch (Exception e) {
            logger.error(append(LogConstant.LGS_FIELD_TAGS,LogConstant.LGS_TAGS_CONF).and(append("batchId",batchId)),LogConstant.LGS_CONF_ERRORMSG_INITBATCH,e );
            r.setResult(false);
            r.setMsg("根据批次加载缓存出错");
        } finally {
            CloseableUtils.closeQuietly(client);
        }

        return r;
    }

    /**
     * 获取批次应该具有的配置版本
     * @param batchId
     * @return
     */
    public String getVersionByBatchId(String batchId){
        //1.根据批次获取所对应的数据的配置类型和配置ID
        Map idMap = zooConfCacheMapper.selectTaskAndSchemaByBatchId(batchId);
        //2.获取配置对应的时间版本
        String schemaId = MapUtil.getCasedString(idMap.get("schema_id"));
        String taskId = MapUtil.getCasedString(idMap.get("task_id"));
        String ifTaskConf = MapUtil.getCasedString(idMap.get("if_self_conf"));
        if(ConfConstant.IF_TASK_CONF.equals(ifTaskConf)){
            schemaId = null;
        }
        //获取配置时间版本
        long version = 0L;
        String lastUpdTime = zooConfCacheMapper.selectMaxUpdTimeForConf(schemaId, taskId);
        if (StringUtils.isNotBlank(lastUpdTime)) {
            version = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").parseDateTime(lastUpdTime).getMillis();
        } else {
            version = System.currentTimeMillis();
        }

        return String.valueOf(version);
    }

    /**
     * 根据批次ID清空其清洗配置
     * @param batchId
     * @return
     */
    public JsonResult<String> removeBatchConf(String batchId){
        CuratorFramework client = ZookeeperUtil.getClient();
        JsonResult<String> r = new JsonResult<>();
        r.setResult(true);
        r.setMsg("清除完成");
        String confPath = ZooCacheConstant.BATCH_PATH+"/"+batchId;
        try {
            if(null!=client.checkExists().forPath(confPath)){
                client.delete().deletingChildrenIfNeeded().forPath(confPath);
            }
        } catch (Exception e) {
            logger.error(append(LogConstant.LGS_FIELD_TAGS,LogConstant.LGS_TAGS_CONF).and(append("batchId",batchId)),LogConstant.LGS_CONF_ERRORMSG_DELETE,e );
        }

        return r;
    }

    /**
     * 根据批次ID获取其清洗配置
     * @param batchId
     * @return
     */
    public JsonResult<String> getBatchConf(String batchId,String confPath){
        CuratorFramework client = ZookeeperUtil.getClient();
        JsonResult<String> r = new JsonResult<>();
        r.setResult(true);
        r.setMsg("获取完成");
        try {
            if(null!=client.checkExists().forPath(confPath)){
                JSONObject service = JSONObject.parseObject(new String(client.getData().forPath(confPath)));
                JSONArray stages = null;
                if(service.containsKey("stages")){
                    stages = service.getJSONArray("stages");
                }
                if(null!=stages && stages.size()>0){
                    JSONArray tmp = new JSONArray();
                    for(int i=0;i<stages.size();i++){
                        String stage = stages.getString(i);
                        String sPath = confPath+"/"+stage;
                        if(null!=client.checkExists().forPath(sPath)){
                            tmp.add(JSONObject.parseObject(new String(client.getData().forPath(sPath))));
                        }
                    }
                    service.put("stagesDetail",tmp);
                }
                r.setData(service.toJSONString());
            }else{
                r.setData("无");
            }
        } catch (Exception e) {
            logger.error(append(LogConstant.LGS_FIELD_TAGS,LogConstant.LGS_TAGS_CONF).and(append("batchId",batchId)),LogConstant.LGS_CONF_ERRORMSG_FETCH,e );
            r.setResult(false);
        }

        return r;
    }

    /**
     * 清除全部流程配置
     *
     * @param client
     * @throws Exception
     */
    private void removeAllConfFromCache(CuratorFramework client) throws Exception {
        if (null != client.checkExists().forPath(ZooCacheConstant.SCHEMA_PATH)) {
            client.delete().deletingChildrenIfNeeded().forPath(ZooCacheConstant.SCHEMA_PATH);
        }
        if (null != client.checkExists().forPath(ZooCacheConstant.TASK_PATH)) {
            client.delete().deletingChildrenIfNeeded().forPath(ZooCacheConstant.TASK_PATH);
        }
    }

    /**
     * 创建全部流程配置缓存
     */
    private void createAllConf(CuratorFramework client) throws Exception {
        List<Map> schemaList = zooConfCacheMapper.selectAllSchemaListForConf();
        List<Map> taskList = zooConfCacheMapper.selectAllTaskListForConf();

        for (Map schema : schemaList) {
            String id = MapUtil.getCasedString(schema.get("id"));
            initConfBySchemaOrTask(client, id, null);
        }
        for (Map task : taskList) {
            String id = MapUtil.getCasedString(task.get("id"));
            initConfBySchemaOrTask(client, null, id);
        }

    }

    /**
     * 根据schemaId或者taskId创建缓存
     *
     * @param schemaId
     * @param taskId
     */
    private void initConfBySchemaOrTask(CuratorFramework client, String schemaId, String taskId) throws Exception {
        String rootPath = StringUtils.isNotBlank(schemaId) ? ZooCacheConstant.SCHEMA_PATH : ZooCacheConstant.TASK_PATH;
        String id = StringUtils.isNotBlank(schemaId) ? schemaId : taskId;

        String rootConf = getStagesConfStr(schemaId, taskId);
        if (StringUtils.isNotBlank(rootConf)) {
            //创建根节点
            client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(rootPath + "/" + id, rootConf.getBytes());
            //创建stage节点
            JSONObject root = JSONObject.parseObject(rootConf);
            if (root.containsKey("stages")) {
                JSONArray stages = root.getJSONArray("stages");
                for (int i = 0; i < stages.size(); i++) {
                    String stageId = stages.getString(i);
                    if (StringUtils.isNotBlank(stageId)) {
                        String stageConfStr = getStageConfStr(schemaId, taskId, stageId);
                        client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(rootPath + "/" + id + "/" + stageId, stageConfStr.getBytes());
                    }
                }
            }
        }
    }

    /**
     * 根据schemaId或者taskId获取配置中的stage信息
     *
     * @param schemaId
     * @param taskId
     * @return
     */
    private String getStagesConfStr(String schemaId, String taskId) {
        JSONObject stageConf = new JSONObject();
        //获取配置时间版本
        long version = 0L;
        String lastUpdTime = zooConfCacheMapper.selectMaxUpdTimeForConf(schemaId, taskId);
        if (StringUtils.isNotBlank(lastUpdTime)) {
            version = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").parseDateTime(lastUpdTime).getMillis();
        } else {
            version = System.currentTimeMillis();
        }
        stageConf.put("version", version);
        //获取stage列表
        List<String> stageList = zooConfCacheMapper.selectStageListForConf(schemaId, taskId);
        JSONArray stages = new JSONArray();
        for (String stage : stageList) {
            stages.add(stage);
        }
        stageConf.put("stages", stages);

        return stageConf.toJSONString();
    }

    /**
     * 获取stage中的流程配置
     *
     * @param schemaId
     * @param taskId
     * @param stageId
     * @return
     */
    private String getStageConfStr(String schemaId, String taskId, String stageId) {
        JSONObject stageConf = new JSONObject();
        List<Map> flows = zooConfCacheMapper.selectConfListByStage(schemaId, taskId, stageId);
        List<String> flowSeq = new ArrayList<>();
        //将所有的流程组成临时map,用于遍历
        Map<String, Map<String, Object>> tempMap = new HashMap<>();
        for (Map flow : flows) {
            if (MapUtil.isBlank(flow.get("parent_id"))) {
                String flowId = MapUtil.getCasedString(flow.get("flow_id"));
                if (tempMap.containsKey(flowId)) {
                    Map<String, Object> moduleMap = tempMap.get(flowId);
                    moduleMap.put("module", flow);
                } else {
                    Map<String, Object> moduleMap = new HashMap<>();
                    moduleMap.put("module", flow);
                    tempMap.put(flowId, moduleMap);
                }
                flowSeq.add(flowId);
            } else {
                String pFlowId = MapUtil.getCasedString(flow.get("parent_id"));
                if (tempMap.containsKey(pFlowId)) {
                    Map<String, Object> moduleMap = tempMap.get(pFlowId);
                    if (moduleMap.containsKey("subModules")) {
                        List<Object> subList = (List<Object>) moduleMap.get("subModules");
                        subList.add(flow);
                    } else {
                        List<Object> subList = new ArrayList<>();
                        subList.add(flow);
                        moduleMap.put("subModules", subList);
                    }
                } else {
                    List<Object> subList = new ArrayList<>();
                    subList.add(flow);
                    Map<String, Object> moduleMap = new HashMap<>();
                    moduleMap.put("subModules", subList);
                    tempMap.put(pFlowId, moduleMap);
                }
            }
        }

        //拼接结果
        JSONArray servArray = new JSONArray();
        for (String flowId : flowSeq) {
            JSONObject flow = new JSONObject();
            Map<String, Object> moduleMap = tempMap.get(flowId);
            Map<String, Object> flowMap = (Map<String, Object>) moduleMap.get("module");

            flow.put("name", StringUtils.defaultString((String) flowMap.get("name")));
            flow.put("descp", StringUtils.defaultString((String) flowMap.get("descp")));
            flow.put("svname", StringUtils.defaultString((String) flowMap.get("sv_name")));
            flow.put("param", StringUtils.defaultString((String) flowMap.get("param")));
            flow.put("testable", flowMap.get("testable"));
            flow.put("flow", flowMap.get("flow_class"));
            Boolean threadable = true;
            JSONArray nodes = new JSONArray();
            if (moduleMap.containsKey("subModules")) {
                List<Map<String, Object>> subList = (List<Map<String, Object>>) moduleMap.get("subModules");
                for (Map<String, Object> subFlow : subList) {
                    nodes.add(subFlow.get("node_class"));
                    if(!"1".equals(MapUtil.getCasedString(subFlow.get("threadable_def")))){
                        //要求子节点必须全部为可多线程时，flow才可以开启多线程
                        threadable = false;
                    }
                }
            } else {
                nodes.add(flowMap.get("node_class"));
            }

            if(threadable && "1".equals(MapUtil.getCasedString(flowMap.get("threadable"))) && "1".equals(MapUtil.getCasedString(flowMap.get("threadable_def")))){
                flow.put("threadable", "1");
            }else{
                flow.put("threadable", "0");
            }
            flow.put("nodes", nodes);

            servArray.add(flow);
        }

        stageConf.put("services", servArray);

        return stageConf.toJSONString();
    }

    /**
     * 根据batchId检查及初始化配置，并返回配置对应路径
     *
     * @param batchId
     * @param client
     * @return
     * @throws Exception
     */
    private String checkAndInitConfCacheByBatchId(String batchId, CuratorFramework client) throws Exception {
        //1.根据批次获取所对应的数据的配置类型和配置ID
        Map idMap = zooConfCacheMapper.selectTaskAndSchemaByBatchId(batchId);
        //2.获取配置对应的时间版本
        String schemaId = MapUtil.getCasedString(idMap.get("schema_id"));
        String taskId = MapUtil.getCasedString(idMap.get("task_id"));
        String ifTaskConf = MapUtil.getCasedString(idMap.get("if_self_conf"));
        //3.判断是否已存在对应版本的流程配置
        JSONObject rootConf = null;
        String rootPath = null;
        if (ConfConstant.IF_TASK_CONF.equals(ifTaskConf)) {
            rootConf = JSONObject.parseObject(getStagesConfStr(null, taskId));
            rootPath = ZooCacheConstant.TASK_PATH + "/" + taskId;
        } else {
            rootConf = JSONObject.parseObject(getStagesConfStr(schemaId, null));
            rootPath = ZooCacheConstant.SCHEMA_PATH + "/" + schemaId;
        }
        if (null != client.checkExists().forPath(rootPath)) {
            //如果存在配置，则判断版本是否一致
            JSONObject root = JSONObject.parseObject(new String(client.getData().forPath(rootPath)));
            //如果版本不一致，则重新加载配置到缓存
            if (!root.getString("version").equals(rootConf.getString("version"))) {
                client.delete().deletingChildrenIfNeeded().forPath(rootPath);
                if (ConfConstant.IF_TASK_CONF.equals(ifTaskConf)) {
                    initConfBySchemaOrTask(client, null, taskId);
                } else {
                    initConfBySchemaOrTask(client, schemaId, null);
                }
            }
        } else {
            //如果不存在配置，则需要创建配置
            if (ConfConstant.IF_TASK_CONF.equals(ifTaskConf)) {
                initConfBySchemaOrTask(client, null, taskId);
            } else {
                initConfBySchemaOrTask(client, schemaId, null);
            }
        }
        return rootPath;
    }

    /**
     * 实例化批次配置
     *
     * @param batchId
     * @param client
     */
    private void makeBatchConfInstance(String batchId, String confPath, CuratorFramework client) throws Exception {
        String rootPath = ZooCacheConstant.BATCH_PATH + "/" + batchId;
        //1.检查批次配置是否存在，如果存在则不进行实例化（正常情况下，在清洗调度结束后由调度程序进行配置的删除）
        if (null == client.checkExists().forPath(rootPath) && null != client.checkExists().forPath(confPath)) {
            JSONObject confObj = JSONObject.parseObject(new String(client.getData().forPath(confPath)));
            confObj.put("status", 0);
            client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(rootPath, confObj.toJSONString().getBytes());

            JSONArray stagesArray = confObj.getJSONArray("stages");
            for (int i = 0; i < stagesArray.size(); i++) {
                String stageId = stagesArray.getString(i);
                if (null != client.checkExists().forPath(confPath + "/" + stageId)) {
                    JSONObject stageObj = JSONObject.parseObject(new String(client.getData().forPath(confPath + "/" + stageId)));
                    JSONArray services = stageObj.getJSONArray("services");
                    for (int j = 0; j < services.size(); j++) {
                        JSONObject service = services.getJSONObject(j);
                        service.put("status", 0);
                        service.put("starttime", "");
                        service.put("endtime", "");
                    }
                    client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(rootPath + "/" + stageId, stageObj.toJSONString().getBytes());
                }
            }

        }
    }

}
