package com.baiyyy.didcs.controller.mng;

import com.baiyyy.didcs.common.constant.ZooCacheConstant;
import com.baiyyy.didcs.common.vo.JsonResult;
import com.baiyyy.didcs.interfaces.flow.ICleanFlow;
import com.baiyyy.didcs.interfaces.node.ICleanNode;
import com.baiyyy.didcs.module.flow.CommonCleanFlowForBatch;
import com.baiyyy.didcs.module.node.CommonArchiveCleanNodeForBatch;
import com.baiyyy.didcs.module.node.CommonInnerMatchCleanNodeForBatch;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

@RestController
public class TestController {
    @RequestMapping(value="/test/flow/innermatch",method= RequestMethod.GET)
    public JsonResult testInnermatch(){
        JsonResult r = new JsonResult();
        try{
            ICleanFlow flow = new CommonCleanFlowForBatch();
            String lockPath = ZooCacheConstant.LOCK_PATH + "/1/" + new Random().nextInt(10);
            flow.init("1","1","1","",null,null,null,200,lockPath);
            ICleanNode node = new CommonInnerMatchCleanNodeForBatch();
            List nodes = new ArrayList();
            nodes.add(node);
            flow.makeChain(nodes);
            flow.start();
            r.setResult(true);
        }catch(Exception e){
            e.printStackTrace();
            r.setResult(false);
        }

        return r;
    }

    @RequestMapping(value="/test/flow/archive",method= RequestMethod.GET)
    public JsonResult testArchive(){
        JsonResult r = new JsonResult();
        try{
            ICleanFlow flow = new CommonCleanFlowForBatch();
            String lockPath = ZooCacheConstant.LOCK_PATH + "/1/" + new Random().nextInt(10);
            flow.init("1","1","1","","538030","538021",null,200,lockPath);
            ICleanNode node = new CommonArchiveCleanNodeForBatch();
            List nodes = new ArrayList();
            nodes.add(node);
            flow.makeChain(nodes);
            flow.start();
            r.setResult(true);
        }catch(Exception e){
            e.printStackTrace();
            r.setResult(false);
        }

        return r;
    }
}
