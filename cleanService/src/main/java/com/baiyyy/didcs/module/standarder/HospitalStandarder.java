package com.baiyyy.didcs.module.standarder;

import com.baiyyy.didcs.common.constant.EsConstant;

/**
 * 医院标准化器
 *
 * @author 逄林
 */
public class HospitalStandarder extends CommonStandarder {

    public HospitalStandarder(){
        super.setIndex(EsConstant.INDEX_NAME_HOSPITAL);
    }
}
