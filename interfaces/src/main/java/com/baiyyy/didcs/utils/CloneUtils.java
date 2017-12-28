package com.baiyyy.didcs.utils;

import java.io.*;

/**
 * Clone工具
 * @author 逄林
 */
public class CloneUtils {

    public static <T extends Serializable> T clone(T obj){
        T clonedObj = null;
        try{
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(obj);
            oos.close();

            ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
            ObjectInputStream ois = new ObjectInputStream(bais);
            clonedObj = (T)ois.readObject();
            ois.close();
        }catch(Exception e){
            e.printStackTrace();
        }
        return clonedObj;
    }

}
