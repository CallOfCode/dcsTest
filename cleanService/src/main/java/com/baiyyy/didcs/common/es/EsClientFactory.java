package com.baiyyy.didcs.common.es;

import com.baiyyy.didcs.common.constant.LogConstant;
import com.baiyyy.didcs.common.listener.SpringPropertyListener;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static net.logstash.logback.marker.Markers.append;

/**
 * HighLevelRestClient工厂类
 * @author 逄林
 */
public class EsClientFactory {
    private static Logger logger = LoggerFactory.getLogger(EsClientFactory.class);

    /**
     * 根据host和port获取RestClient
     * @param hostName
     * @param port
     * @return
     */
    public static RestHighLevelClient getRestClient(String hostName,Integer port){
        RestHighLevelClient client = null;
        try{
            client = new RestHighLevelClient(RestClient.builder(
                    new HttpHost(hostName,port,"http")
            ));
        }catch(Exception e){
            logger.error(append(LogConstant.LGS_FIELD_TAGS,LogConstant.LGS_TAGS_ES),LogConstant.LGS_ES_ERRORMSG_CLIENT,e);
        }
        return client;
    }

    /**
     * 获取默认client
     * @return
     */
    public static RestHighLevelClient getDefaultRestClient(){
        return getRestClient(SpringPropertyListener.getPropertyValue("${elasticsearch.restClient-url}"),Integer.parseInt(SpringPropertyListener.getPropertyValue("${elasticsearch.restClient-port}")));
    }
}
