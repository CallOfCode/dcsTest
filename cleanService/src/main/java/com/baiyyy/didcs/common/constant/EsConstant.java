package com.baiyyy.didcs.common.constant;

/**
 * Elasticsearch 常量
 *
 * @author 逄林
 */
public class EsConstant {

    /**
     * index前缀
     */
    public static final String INDEX_PREFIX = "clean_";
    /**
     * 系统index前缀
     */
    public static final String INDEX_PREFIX_CLEAN_LOG = "clean_log_";
    /**
     * 初始化时的锁路径前缀
     */
    public static final String INDEX_INIT_LOCK_PREFIX = "/es_cache_lock_";
    /**
     * 默认type 名
     */
    public static final String DEFAULT_TYPE = "doc";
    /**
     * 默认别名字段名称
     */
    public static final String DEFAULT_ALIAS_FIELDNAME = "alias_name";
    /**
     * 源数据日志
     */
    public static final String LOG_TARGET_SOURCE = "source";
    /**
     * 标准数据日志
     */
    public static final String LOG_TARGET_STD = "std";

    /**
     * ES 执行perform命令时的正常返回值
     */
    public static Integer ES_STATUS_OK = 200;
    /**
     * 医生索引名
     */
    public static String INDEX_NAME_DOCTOR = "doctor";
    /**
     * 医院索引名
     */
    public static String INDEX_NAME_HOSPITAL = "hospital";
    /**
     * 科室索引名
     */
    public static String INDEX_NAME_SECTION = "section";
    /**
     * 职称索引名
     */
    public static String INDEX_NAME_TITLE = "title";
    /**
     * 职务索引名
     */
    public static String INDEX_NAME_POSITION = "position";
    /**
     * 性别索引名
     */
    public static String INDEX_NAME_GENDER = "gender";
    /**
     * 省索引名
     */
    public static String INDEX_NAME_PROVINCE = "province";
    /**
     * 市索引名
     */
    public static String INDEX_NAME_CITY = "city";
    /**
     * 区索引名
     */
    public static String INDEX_NAME_COUNTY = "county";
}
