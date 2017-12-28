package com.baiyyy.didcs.common.constant;

/**
 * 清洗过程常量
 *
 * @author 逄林
 */
public class FlowConstant {
    /**
     * 清洗状态，未开始
     */
    public static final Integer CLEAN_STATUS_NOT_BEGIN = 0;
    /**
     * 清洗状态，清洗中
     */
    public static final Integer CLEAN_STATUS_CLEANING = 1;
    /**
     * 清洗状态，中止
     */
    public static final Integer CLEAN_STATUS_STOP = -1;
    /**
     * 服务未启动
     */
    public static final Integer SERVICE_STATUS_NOT_BEGIN = 0;
    /**
     * 服务运行中
     */
    public static final Integer SERVICE_STATUS_CLEANING = 1;
    /**
     * 服务已完成
     */
    public static final Integer SERVICE_STATUS_END = 2;
    /**
     * 流程每批次处理数据数量
     */
    public static final Integer FLOW_HANDLE_SIZE = 200;
    /**
     * 待处理有效数据
     */
    public static final String DATA_STATUS_VALID = "1";
    /**
     * 入库
     */
    public static final String DATA_STATUS_STO = "2";
    /**
     * 入库审核
     */
    public static final String DATA_STATUS_STO_CHECK = "20";
    /**
     * 组内关联
     */
    public static final String DATA_STATUS_INNER_MATCH = "3";
    /**
     * 匹配
     */
    public static final String DATA_STATUS_MATCH = "4";
    /**
     * 疑似
     */
    public static final String DATA_STATUS_LIKE = "5";
    /**
     * 已删除
     */
    public static final String DATA_STATUS_DEL = "0";
    /**
     * 置疑数据
     */
    public static final String DATA_STATUS_NOT_SURE = "9";
    /**
     * 格式化阶段
     */
    public static final String CLEAN_STAGE_FMT = "fmt";
    /**
     * 标准化阶段
     */
    public static final String CLEAN_STAGE_STD = "std";
    /**
     * 匹配阶段
     */
    public static final String CLEAN_STAGE_MATCH = "match";
    /**
     * 查重阶段
     */
    public static final String CLEAN_STAGE_INNERMATCH = "innermatch";
}
