package com.baiyyy.didcs.service.dispatcher;

import com.baiyyy.didcs.common.constant.LogConstant;
import com.baiyyy.didcs.common.util.SpringContextUtil;
import com.baiyyy.didcs.common.util.ZookeeperUtil;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.shared.SharedCount;
import org.apache.curator.utils.CloseableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.concurrent.*;

import static net.logstash.logback.marker.Markers.append;

/**
 * 共享锁处理service
 *
 * @author 逄林
 */
@Service
public class LockService {
    Logger logger = LoggerFactory.getLogger(LockService.class);
    /**
     * 共享锁-1
     * @param lockPath
     * @param batchId
     * @param userId
     */
    public void subLockNum(String lockPath,String batchId,String userId){
        CuratorFramework client = ZookeeperUtil.getClient();
        SharedCount counter = new SharedCount(client,lockPath,0);
        try {
            counter.start();
            //计数器-1
            int count = counter.getCount();
            counter.setCount(counter.getCount()-1);
            count = counter.getCount();
            if(count==0){
                //当计数器变为0时，需要结束当前service并开始下一service
                //结束service由获得锁的线程执行
                InterProcessMutex lock = new InterProcessMutex(client,lockPath);
                //重试3次
                int retry = 3;
                while(retry>0){
                    if(lock.acquire(1000, TimeUnit.MILLISECONDS)){
                        count = counter.getCount();
                        //再次判断计数器值
                        if(count==0){
                            counter.setCount(-1);
                            ThreadFactory namedThreadFactory = new ThreadFactoryBuilder()
                                    .setNameFormat("批次:"+batchId+":服务线程-%d").build();
                            ExecutorService service = new ThreadPoolExecutor(1, 1,
                                    0L, TimeUnit.MILLISECONDS,
                                    new LinkedBlockingQueue<Runnable>(),namedThreadFactory);
                            service.submit(new Runnable() {
                                @Override
                                public void run() {
                                    //调用后续流程
                                    CleanDispatcherService cleanDispatcherService = SpringContextUtil.getBean("cleanDispatcherService");
                                    cleanDispatcherService.stopAndDispatch(batchId,userId);
                                }
                            });
                            service.shutdown();
                        }
                        lock.release();

                        break;
                    }
                    retry--;
                }

            }
        } catch (Exception e1) {
            logger.error(append(LogConstant.LGS_FIELD_TAGS,LogConstant.LGS_TAGS_LOCK).and(append("batchId",batchId)).and(append("lockPath",lockPath)),LogConstant.LGS_LOCK_ERRORMSG_SUB,e1 );
        }finally {
            CloseableUtils.closeQuietly(counter);
            CloseableUtils.closeQuietly(client);
        }
    }

    /**
     * 为指定路径创建分布式锁，并尝试加锁，如果加锁成功，则返回锁，否则返回null
     * @param client
     * @param lockPath
     * @return
     */
    public InterProcessMutex createLock(CuratorFramework client, String lockPath){
        InterProcessMutex lock = null;
        try{
            lock = new InterProcessMutex(client,lockPath);
            if(lock.acquire(1000, TimeUnit.MILLISECONDS)){

            }else{
                return null;
            }
        }catch(Exception e){
            logger.error(append(LogConstant.LGS_FIELD_TAGS,LogConstant.LGS_TAGS_LOCK).and(append("lockPath",lockPath)),LogConstant.LGS_LOCK_ERRORMSG_CREATE,e );
            lock = null;
        }
        return lock;
    }

}
