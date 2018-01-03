package com.baiyyy.didcs.service;

import com.baiyyy.didcs.dao.IndexMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class IndexService {
    @Autowired
    @SuppressWarnings("SpringJavaAutowiringInspection")
    private IndexMapper indexMapper;

    public Integer getSchemaCount(){
        return indexMapper.selectSchemaCount();
    }

    public Integer getTaskCount(){
        return indexMapper.selectTaskCount();
    }

    public Integer getBatchCount(){
        return indexMapper.selectBatchCount();
    }

    public Integer getRunBatchCount(){
        return indexMapper.selectRunBatchCount();
    }

}
