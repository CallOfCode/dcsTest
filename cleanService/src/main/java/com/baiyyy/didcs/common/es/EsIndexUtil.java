package com.baiyyy.didcs.common.es;

import com.alibaba.fastjson.JSONObject;
import com.baiyyy.didcs.common.constant.EsConstant;
import com.baiyyy.didcs.common.constant.LogConstant;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;

import static net.logstash.logback.marker.Markers.append;

/**
 * elasticsearch index相关操作
 *
 * @author 逄林
 */
public class EsIndexUtil {
    private static Logger logger = LoggerFactory.getLogger(EsIndexUtil.class);

    /**
     * 检查索引是否存在
     * @param restClient
     * @param index
     * @return
     * @throws Exception
     */
    public static Boolean existsIndex(RestHighLevelClient restClient, String index) throws Exception{
        Boolean result = null;
        Response response = null;
        try {
            response = restClient.getLowLevelClient().performRequest("HEAD",index, Collections.<String, String>emptyMap());
            int code = response.getStatusLine().getStatusCode();
            if(code== EsConstant.ES_STATUS_OK){
                result = true;
            }else{
                result = false;
            }
        } catch (IOException e) {
            logger.error(append(LogConstant.LGS_FIELD_TAGS,LogConstant.LGS_TAGS_ES),LogConstant.LGS_ES_ERRORMSG_INDEXCHECK,e );
            throw e;
        }
        return result;
    }

    /**
     * 删除索引
     * @param restClient
     * @param index
     * @throws Exception
     */
    public static void deleteIndex(RestHighLevelClient restClient,String index) throws Exception{
        try {
            restClient.getLowLevelClient().performRequest("DELETE",index,Collections.<String, String>emptyMap());
        }catch (Exception e){
            logger.error(append(LogConstant.LGS_FIELD_TAGS,LogConstant.LGS_TAGS_ES),LogConstant.LGS_ES_ERRORMSG_INDEXDELETE,e );
            throw e;
        }
    }

    /**
     * 创建索引
     * @param restClient
     * @param index
     * @param indexBody
     * @throws Exception
     */
    public static void createIndex(RestHighLevelClient restClient,String index,String indexBody) throws Exception{
        try {
            HttpEntity entity = new NStringEntity(indexBody, ContentType.APPLICATION_JSON);
            restClient.getLowLevelClient().performRequest("PUT",index,Collections.emptyMap(),entity);
        }catch (Exception e){
            logger.error(append(LogConstant.LGS_FIELD_TAGS,LogConstant.LGS_TAGS_ES),LogConstant.LGS_ES_ERRORMSG_INDEX,e );
            throw e;
        }
    }

    public static String getIndexMappingInfo(RestHighLevelClient restClient,String index) throws Exception{
         String str = null;
        try {
            str = EntityUtils.toString(restClient.getLowLevelClient().performRequest("GET",index+"/_mapping",Collections.emptyMap()).getEntity());
        }catch (Exception e){
            logger.error(append(LogConstant.LGS_FIELD_TAGS,LogConstant.LGS_TAGS_ES),LogConstant.LGS_ES_ERRORMSG_INDEX_MAPPING,e );
            throw e;
        }
        return str;
    }

    public static void addMappingInfo(RestHighLevelClient restClient,String index,String jsonStr) throws Exception{
        try{
            StringBuffer sb = new StringBuffer()
                    .append("{")
                    .append("\"properties\":{")
                    .append(jsonStr)
                    .append("}")
                    .append("}");

            JSONObject response = JSONObject.parseObject(EntityUtils.toString(restClient.getLowLevelClient().performRequest("PUT",index+"/_mapping/doc",Collections.emptyMap(),new NStringEntity(sb.toString(), ContentType.APPLICATION_JSON)).getEntity()));
        }catch(Exception e){
            logger.error(append(LogConstant.LGS_FIELD_TAGS,LogConstant.LGS_TAGS_ES),LogConstant.LGS_ES_ERRORMSG_MAPPING_ADD,e );
            throw e;
        }
    }

}
