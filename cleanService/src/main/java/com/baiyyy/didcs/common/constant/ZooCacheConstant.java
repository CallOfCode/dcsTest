package com.baiyyy.didcs.common.constant;

/**
 * 跟ZooCache相关的常量
 *
 * @author 逄林
 */
public class ZooCacheConstant {
    /**
     * 并发线程数量
     */
    public static final Integer THREAD_NUMS = 1;
    /**
     * 元数据的配置根路径
     */
    public static final String SCHEMA_PATH = "/clean_init_conf/schema";
    /**
     * 任务的配置根路径
     */
    public static final String TASK_PATH = "/clean_init_conf/task";
    /**
     * 批次的清洗配置根路径
     */
    public static final String BATCH_PATH = "/clean_run_conf";
    /**
     * 资源锁定根路径
     */
    public static final String LOCK_PATH = "/clean_lock";
}
