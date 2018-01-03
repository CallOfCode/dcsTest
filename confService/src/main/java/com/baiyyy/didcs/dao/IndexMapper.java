package com.baiyyy.didcs.dao;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;

@Mapper
public interface IndexMapper extends BaseMapper {
    @Select("SELECT count(id) from t_biz_schema WHERE IFNULL(available,0)=1")
    public int selectSchemaCount();
    @Select("SELECT count(id) from t_biz_task WHERE IFNULL(available,0)=1")
    public int selectTaskCount();
    @Select("SELECT count(id) from t_biz_batch WHERE IFNULL(available,0)=1")
    public int selectBatchCount();
    @Select("SELECT count(id) from t_biz_batch WHERE IFNULL(available,0)=1 and IFNULL(if_start,0)=1")
    public int selectRunBatchCount();
}
