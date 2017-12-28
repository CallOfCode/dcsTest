package com.baiyyy.didcs.eureka.listener;

import com.netflix.appinfo.InstanceInfo;
import com.netflix.discovery.shared.Applications;
import com.netflix.eureka.EurekaServerContextHolder;
import com.netflix.eureka.registry.PeerAwareInstanceRegistry;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;
import org.springframework.cloud.netflix.eureka.server.event.EurekaInstanceCanceledEvent;
import org.springframework.cloud.netflix.eureka.server.event.EurekaInstanceRegisteredEvent;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;

import static net.logstash.logback.marker.Markers.append;

/**
 * 监听服务启动和停机事件
 *
 * @author 逄林
 */
@Configuration
@EnableScheduling
@Slf4j
public class EurekaUpDownListener implements ApplicationListener{
    @Override
    public void onApplicationEvent(ApplicationEvent applicationEvent) {
        if(applicationEvent instanceof EurekaInstanceRegisteredEvent){
            EurekaInstanceRegisteredEvent event = (EurekaInstanceRegisteredEvent)applicationEvent;
            log.warn(append("tags","eureka").and(append("serviceName",event.getInstanceInfo().getAppName())),"服务启动");
            lostInstanceMap.remove(event.getInstanceInfo().getInstanceId());
        }else if(applicationEvent instanceof EurekaInstanceCanceledEvent){
            EurekaInstanceCanceledEvent event = (EurekaInstanceCanceledEvent) applicationEvent;
            PeerAwareInstanceRegistry registry = EurekaServerContextHolder.getInstance().getServerContext().getRegistry();
            Applications applications = registry.getApplications();
            applications.getRegisteredApplications().forEach((registeredApplication)->{
                registeredApplication.getInstances().forEach((instance)->{
                    if(instance.getInstanceId().equals(event.getServerId())){
                        String id = instance.getInstanceId();
                        lostInstanceMap.remove(id);
                        lostInstanceMap.put(id,new LostInstance(instance));
                    }
                });
            });
            log.warn(append("tags","eureka").and(append("serviceName",event.getAppName())).and(append("serverId",event.getServerId())),"服务关闭");
        }

    }

    private ConcurrentHashMap<String,LostInstance> lostInstanceMap = new ConcurrentHashMap<>();

    private int[] defalutNotifyInterval = {0,60,120,240,480};

    @Scheduled(cron = "0/30 * * * * ?")
    private void notifyLostInstance(){
        lostInstanceMap.entrySet().forEach((lostInstanceMap)->{
            String key = lostInstanceMap.getKey();
            LostInstance instance = lostInstanceMap.getValue();
            DateTime dt = new DateTime(instance.getLostTime());
            if(dt.plusSeconds(defalutNotifyInterval[instance.getCurrentInterval()]).isBeforeNow()){
                log.warn(append("tags","eureka").and(append("serviceId",instance.getInstanceId())).and(append("serverIp",instance.getIPAddr())).and(append("closeTime",dt.toString())),"服务失效，请马上重启服务");
                //TODO 可以增加消息提醒
            }
        });
    }

    private void getInstanceStatus(){

    }

    class LostInstance extends InstanceInfo{
        protected int currentInterval = 0;
        protected Date lostTime;
        public LostInstance(InstanceInfo ii) {
            super(ii);
            this.lostTime = new Date();
        }

        public Date getLostTime() {
            return lostTime;
        }

        public void setLostTime(Date lostTime) {
            this.lostTime = lostTime;
        }
        public int getCurrentInterval(){
            return currentInterval++%4;
        }
    }
}




