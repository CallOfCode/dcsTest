package com.baiyyy.didcs.service.dispatcher;

import com.baiyyy.didcs.common.constant.ConfConstant;
import com.baiyyy.didcs.common.constant.LogConstant;
import com.baiyyy.didcs.common.util.MapUtil;
import com.baiyyy.didcs.common.vo.JsonResult;
import com.baiyyy.didcs.dao.dispatcher.SchemaTaskMapper;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

import static net.logstash.logback.marker.Markers.*;

/**
 * 与schema、task操作相关的service
 *
 * @author 逄林
 */
@Service
public class SchemaTaskService {
    Logger logger = LoggerFactory.getLogger(SchemaTaskService.class);

    @SuppressWarnings("SpringJavaAutowiringInspection")
    @Autowired
    private SchemaTaskMapper schemaTaskMapper;

    /**
     * 根据taskId创建表
     *
     * @param taskId
     * @return
     */
    public JsonResult<String> createTableForTask(String taskId) {
        JsonResult<String> r = new JsonResult<>();
        r.setResult(true);
        if (StringUtils.isBlank(taskId)) {
            r.setResult(false);
            r.setMsg("ID为空");
            return r;
        }

        //1.获取schema表名
        String sourceTableName = null;
        String stdTableName = null;
        Map tableMap = schemaTaskMapper.selectTableNameByTaskId(taskId);
        if (null != tableMap && !tableMap.isEmpty()) {
            sourceTableName = new StringBuffer().append(tableMap.get("source_code")).append("_").append(tableMap.get("code")).toString();
            stdTableName = MapUtil.getCasedString(tableMap.get("std_code"));
        } else {
            r.setResult(false);
            r.setMsg("表名不存在");
            return r;
        }

        //2.获取字段及属性，组装sql
        List<Map> schemaFields = schemaTaskMapper.selectTableFieldsByTaskId(taskId);
        List<Map> commonFields = schemaTaskMapper.selectCommonTableFields();
        List<Map> stdSchemaFields = schemaTaskMapper.selectStdTableFieldByTableName(stdTableName);

        String sourceTableSql = getGenSourceTableSql(schemaFields, commonFields, sourceTableName);
        String stdTableSql = getGenStdTableSql(stdSchemaFields, stdTableName);
        String stdAliasSql = getGenStdAliasTableSql(stdTableName);
        try {
            if (StringUtils.isNoneBlank(sourceTableSql)) {
                schemaTaskMapper.execSql(sourceTableSql);
            }
            if (StringUtils.isNoneBlank(stdTableSql)) {
                schemaTaskMapper.execSql(stdTableSql);
            }
            if (StringUtils.isNoneBlank(stdAliasSql)) {
                schemaTaskMapper.execSql(stdAliasSql);
            }
            logger.info(append(LogConstant.LGS_FIELD_TAGS,LogConstant.LGS_TAGS_SCHEMA).and(append("taskId",taskId)),LogConstant.LGS_SCHEMA_SUCCMSG_CRTABLE );
        } catch (Exception e) {
            logger.error(append(LogConstant.LGS_FIELD_TAGS,LogConstant.LGS_TAGS_SCHEMA).and(append("taskId",taskId)),LogConstant.LGS_SCHEMA_ERRORMSG_CRTABLE,e );
            r.setResult(false);
            r.setMsg("建表出错");
        }

        return r;
    }

    /**
     * 获取创建源数据表的语句
     *
     * @param schemaFields
     * @param commonFields
     * @param sourceTableName
     * @return
     */
    private String getGenSourceTableSql(List<Map> schemaFields, List<Map> commonFields, String sourceTableName) {
        if (null == schemaFields || schemaFields.size() == 0) {
            return null;
        }

        StringBuffer sb = new StringBuffer();
        sb.append("create table if not exists ").append(sourceTableName)
                .append("(")
                .append("id bigint(20) not null auto_increment,");
        for (Map schema : schemaFields) {
            if (ConfConstant.FIELD_NOT_STD.equals(MapUtil.getCasedString(schema.get("if_std")))) {
                sb.append(schema.get("code")).append(" ").append(schema.get("attr_type")).append("(").append(schema.get("attr_length")).append("),");
                sb.append(schema.get("code")).append("_stdstr").append(" ").append(schema.get("attr_type")).append("(").append(schema.get("attr_length")).append("),");
            } else {
                sb.append(schema.get("code")).append(" ").append(schema.get("attr_type")).append("(").append(schema.get("attr_length")).append("),");
                sb.append(schema.get("code")).append("_stdstr").append(" ").append(schema.get("attr_type")).append("(").append(schema.get("attr_length")).append("),");
                sb.append(schema.get("code")).append("_stdid").append(" ").append("bigint(20),");
            }
        }

        for (Map common : commonFields) {
            if (ConfConstant.FIELD_NOT_STD.equals(MapUtil.getCasedString(common.get("if_std")))) {
                sb.append(common.get("code")).append(" ").append(common.get("attr_type")).append("(").append(common.get("attr_length")).append("),");
            } else {
                sb.append(common.get("code")).append(" ").append(common.get("attr_type")).append("(").append(common.get("attr_length")).append("),");
                sb.append(common.get("code")).append("_stdstr").append(" ").append(common.get("attr_type")).append("(").append(common.get("attr_length")).append("),");
                sb.append(common.get("code")).append("_stdid").append(" ").append("bigint(20),");
            }
        }
        sb.append("primary key (id)").append(")");

        return sb.toString();
    }

    /**
     * 获取创建标准数据表的语句
     *
     * @param schemaFields
     * @param stdTableName
     * @return
     */
    private String getGenStdTableSql(List<Map> schemaFields, String stdTableName) {
        if (null == schemaFields || schemaFields.size() == 0) {
            return null;
        }
        StringBuffer sb = new StringBuffer();
        sb.append("create table if not exists ").append(stdTableName)
                .append("(")
                .append("id bigint(20) not null AUTO_INCREMENT,");
        for (Map schema : schemaFields) {
            if (ConfConstant.FIELD_NOT_STD.equals(MapUtil.getCasedString(schema.get("if_std")))) {
                sb.append(schema.get("code")).append(" ").append(schema.get("attr_type")).append("(").append(schema.get("attr_length")).append("),");
            } else {
                sb.append(schema.get("code")).append(" ").append(schema.get("attr_type")).append("(").append(schema.get("attr_length")).append("),");
                sb.append(schema.get("code")).append("_id").append(" ").append("bigint(20),");
            }
        }
        sb.append("source_batch_id bigint(20),source_data_id bigint(20),").append("add_time varchar(24),").append("upd_time varchar(24),");
        sb.append("primary key (id)").append(")");

        return sb.toString();
    }

    private String getGenStdAliasTableSql(String stdTableName) {
        StringBuffer sb = new StringBuffer();
        sb.append("create table if not exists ").append(stdTableName).append("_alias")
                .append("(")
                .append("id bigint(20) not null AUTO_INCREMENT,")
                .append("std_id bigint(20),")
                .append("alias_name varchar(100),")
                .append("alias_ref varchar(100),")
                .append("sys varchar(20),")
                .append("upd_time varchar(24),")
                .append("primary key (id)")
                .append(")");

        return sb.toString();
    }

}
