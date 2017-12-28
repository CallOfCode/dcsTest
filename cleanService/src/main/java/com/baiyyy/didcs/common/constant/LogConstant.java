package com.baiyyy.didcs.common.constant;

/**
 * 日志记录常量
 *
 * @author 逄林
 */
public class LogConstant {

    /**
     * logstash 标签-tags
     */
    public static String LGS_FIELD_TAGS = "tags";

    /***==================schema相关 begin===================*/
    /**
     * 配置标签值
     */
    public static String LGS_TAGS_SCHEMA = "schema";
    /**
     * 使用schema创建数据表
     */
    public static String LGS_SCHEMA_SUCCMSG_CRTABLE_BEGIN = "使用schema创建数据表";
    /**
     * 建表出错
     */
    public static String LGS_SCHEMA_ERRORMSG_CRTABLE = "建表出错";
    /**
     * 建表成功
     */
    public static String LGS_SCHEMA_SUCCMSG_CRTABLE = "建表成功";
    /***==================schema相关 end===================*/

    /***==================调度相关 begin===================*/
    /**
     * 调度标签值
     */
    public static String LGS_TAGS_DISPATCH = "dispatch";
    /**
     * 调度下一服务错误消息
     */
    public static String LGS_DISPATCH_ERRORMSG_NEXT = "调度下一服务出错";

    /**
     * 停止当前服务并调度下一服务错误消息
     */
    public static String LGS_DISPATCH_ERRORMSG_STOP_NEXT = "停止当前服务并调度下一服务出错";
    /**
     * 检查并初始化设置出错
     */
    public static String LGS_DISPATCH_ERRORMSG_INIT = "检查并初始化清洗配置出错";
    /**
     * 发起服务调用
     */
    public static String LGS_DISPATCH_SUCCMSG_SVINVOKE = "发起服务调用";
    /**
     * 清洗开始
     */
    public static String LGS_DISPATCH_SUCCMSG_SVBEGIN = "清洗开始";
    /**
     * 因配置导致服务调用失败
     */
    public static String LGS_DISPATCH_ERRORMSG_SVINVOKE_FAILED = "因配置导致服务调用失败";
    /**
     * 服务调用出错
     */
    public static String LGS_DISPATCH_ERRORMSG_SVINVOKE = "服务调用出错";
    /**
     * 服务结束出错
     */
    public static String LGS_DISPATCH_ERRORMSG_FINISH = "结束清洗调度出错";
    /**
     * 服务结束出错
     */
    public static String LGS_DISPATCH_SUCCMSG_FINISH = "清洗调度完成";
    /***==================调度相关 end===================*/
    /***==================配置相关 begin===================*/
    /**
     * 配置标签值
     */
    public static String LGS_TAGS_CONF = "config";
    /**
     * 检查并初始化设置出错
     */
    public static String LGS_CONF_ERRORMSG_INITALL = "初始化全部配置出错";
    /**
     * 检查并初始化设置出错
     */
    public static String LGS_CONF_ERRORMSG_INITBATCH = "初始化批次配置出错";
    /**
     * 删除设置出错
     */
    public static String LGS_CONF_ERRORMSG_DELETE = "删除批次配置出错";
    /**
     * 获取设置出错
     */
    public static String LGS_CONF_ERRORMSG_FETCH = "获取批次配置出错";
    /***==================配置相关 end===================*/
    /***==================Flow 相关 begin===================*/
    /**
     * 配置标签值
     */
    public static String LGS_TAGS_FLOW = "flow";
    /**
     * FlowInvoker调用出错
     */
    public static String LGS_FLOW_ERRORMSG_INVOKER = "FlowInvoker调用出错";
    /**
     * 创建计数器出错
     */
    public static String LGS_FLOW_ERRORMSG_COUNTER = "创建计数器出错";
    /**
     * 服务线程启动出错
     */
    public static String LGS_FLOW_ERRORMSG_THREAD = "服务线程启动出错";
    /**
     * 拆分线程中值出错
     */
    public static String LGS_FLOW_ERRORMSG_MIDDLE = "拆分线程中值出错";
    /**
     * Loop后续处理出错
     */
    public static String LGS_FLOW_ERRORMSG_AFTERLOOP = "Loop后续处理出错";
    /***==================Flow 相关 end===================*/
    /***==================Node 相关 begin===================*/
    /**
     * 配置标签值
     */
    public static String LGS_TAGS_NODE = "node";
    /**
     * node执行出错
     */
    public static String LGS_NODE_ERRORMSG_EXEC = "node执行出错";
    /**
     * node执行标准化出错
     */
    public static String LGS_NODE_ERRORMSG_STD = "node执行标准化出错";
    /**
     * node执行匹配出错
     */
    public static String LGS_NODE_ERRORMSG_MATCH = "node执行匹配出错";
    /**
     * node执行归档出错
     */
    public static String LGS_NODE_ERRORMSG_STO = "node执行归档出错";
    /**
     * node执行
     */
    public static String LGS_NODE_SUCCMSG_EXEC = "node执行";
    /***==================Node 相关 end===================*/

    /***==================Lock 相关 begin===================*/
    /**
     * 配置标签值
     */
    public static String LGS_TAGS_LOCK = "lock";
    /**
     * 计数器-1出错
     */
    public static String LGS_LOCK_ERRORMSG_SUB = "计数器-1出错";
    /**
     * 锁创建出错
     */
    public static String LGS_LOCK_ERRORMSG_CREATE = "锁创建出错";
    /***==================Lock 相关 end===================*/

    /***==================Es 相关 begin===================*/
    /**
     * 配置标签值
     */
    public static String LGS_TAGS_ES = "elasticsearch";
    /**
     * 创建client出错
     */
    public static String LGS_ES_ERRORMSG_CLIENT = "创建client出错";
    /**
     * 索引出错
     */
    public static String LGS_ES_ERRORMSG_INDEX = "索引出错";
    /**
     * 更新出错
     */
    public static String LGS_ES_ERRORMSG_UPDATE = "更新出错";
    /**
     * 查询索引mapping出错
     */
    public static String LGS_ES_ERRORMSG_INDEX_MAPPING = "查询索引mapping出错";

    /**
     * 添加mapping出错
     */
    public static String LGS_ES_ERRORMSG_MAPPING_ADD = "添加mapping出错";
    /**
     * 批量索引出错
     */
    public static String LGS_ES_ERRORMSG_INDEX_BULK = "批量索引出错";
    /**
     * 批量更新出错
     */
    public static String LGS_ES_ERRORMSG_UPDATE_BULK = "批量更新出错";
    /**
     * 索引检查出错
     */
    public static String LGS_ES_ERRORMSG_INDEXCHECK = "索引检查出错";
    /**
     * 索引删除
     */
    public static String LGS_ES_ERRORMSG_INDEXDELETE = "索引删除出错";
    /**
     * 释放锁出错
     */
    public static String LGS_ES_ERRORMSG_RELEASELOCK = "释放锁出错";
    /**
     * 释放RestClient出错
     */
    public static String LGS_ES_ERRORMSG_RELEASERESTCLIENT = "释放RestClient出错";
    /**
     * 缓存初始化开始
     */
    public static String LGS_ES_SUCCMSG_INIT_BEGIN = "缓存初始化开始";
    /**
     * 缓存刷新开始
     */
    public static String LGS_ES_SUCCMSG_REFRESH_BEGIN = "缓存刷新开始";
    /**
     * 缓存初始化完成
     */
    public static String LGS_ES_SUCCMSG_INIT_FINISH = "缓存初始化完成";
    /**
     * 缓存刷新完成
     */
    public static String LGS_ES_SUCCMSG_REFRESH_FINISH = "缓存刷新完成";
    /**
     * 缓存初始化被其他线程占用，当前结束
     */
    public static String LGS_ES_SUCCMSG_INIT_BLOCK = "缓存初始化被其他线程占用，当前结束";
    /**
     * 缓存刷新被其他线程占用，当前结束
     */
    public static String LGS_ES_SUCCMSG_REFRESH_BLOCK = "缓存刷新被其他线程占用，当前结束";
    /***==================Es 相关 end===================*/
    /***==================Log 相关 begin===================*/
    /**
     * 配置标签值
     */
    public static String LGS_TAGS_LOG = "DataLog";
    /**
     * 使用schema创建数据表
     */
    public static String LGS_LOG_ERRMSG_LOG = "日志记录出错";

    /***==================Log 相关 end===================*/
}
