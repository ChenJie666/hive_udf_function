package com.iotmars.hive;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.logging.Logger;

/**
 * @author CJ
 * @date: 2022/6/16 10:38
 */
public class ParseErrorCodeAndGetDiff extends GenericUDTF {

    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        // 1. 首先检查参数合法性
        if (argOIs.length != 2) {
            throw new UDFArgumentException("只需要两个参数");
        }

        if (argOIs[0].getCategory() != ObjectInspector.Category.PRIMITIVE || argOIs[1].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentException("只接受基础类型参数");
        }

        // 2. 定义返回值名称和类型
        ArrayList<String> fieldName = new ArrayList<>();
        fieldName.add("error_code");

        ArrayList<ObjectInspector> fieldType = new ArrayList<>();
        fieldType.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldName, fieldType);
    }

    public void process(Object[] objects) throws HiveException {
        try {
            Long newErrorCode = Long.parseLong(objects[0].toString());
            ArrayList<String> newFaultCodeList = parseErrorCode(newErrorCode);

            Long oldErrorCode = Long.parseLong(objects[1].toString());
            ArrayList<String> oldFaultCodeList = parseErrorCode(oldErrorCode);

            // 返回新增的异常值
            for (String newFaultCode : newFaultCodeList) {
                if (!oldFaultCodeList.contains(newFaultCode)) {
                    forward(newFaultCode);
                }
            }

        } catch (Exception e) {
            Logger.getLogger("ParseErrorCodeAndGetDiff").warning("解析错误代码异常: " + objects[0] + "  " + objects[1]);
        }

    }

    private ArrayList<String> parseErrorCode(Long code){
        ArrayList<String> list = new ArrayList<>();

        long exp = 0L;
        long value = 0L;
        while (code > value) {
            // 通过比较
            value = Double.valueOf(Math.pow(2D, exp)).longValue();
            long calc = code & value;
            if (calc > 0L) {
                list.add(String.valueOf(exp));
            }
            ++exp;
        }

        // 转化为指数上标的集合
        return list;
    }

    public void close() throws HiveException {
    }
}
