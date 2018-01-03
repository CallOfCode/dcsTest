package com.baiyyy.didcs.dao;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.util.List;
import java.util.Map;

/**
 * Mapper 基类
 *
 * @author 逄林
 */
@Mapper
public interface BaseMapper {

    /**
     * 执行语句
     * @param sql
     */
    @Update("${sql}")
    public void updateBySql(@Param("sql") String sql);

}
