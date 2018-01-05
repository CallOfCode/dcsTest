package com.baiyyy.didcs.service;

import com.baiyyy.didcs.dao.SchemaMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class SchemaService {
    @Autowired
    @SuppressWarnings("SpringJavaAutowiringInspection")
    private SchemaMapper schemaMapper;

    public List<Map> getPageSchemas(String name, Integer limit, Integer offset){
        return schemaMapper.selectPageSchemas(name,limit,offset);
    }

}
