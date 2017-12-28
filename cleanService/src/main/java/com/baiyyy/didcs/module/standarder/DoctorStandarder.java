package com.baiyyy.didcs.module.standarder;

import com.baiyyy.didcs.common.constant.EsConstant;

/**
 * 医生标准化器
 *
 * @author 逄林
 */
public class DoctorStandarder extends CommonStandarder {

    public DoctorStandarder(){
        super.setIndex(EsConstant.INDEX_NAME_DOCTOR);
    }
}
