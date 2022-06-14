package com.iotmars.hive;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;

/**
 * 用于飞燕数据，比较两个json串获取差异的属性
 *
 * @author CJ
 * @date: 2022/6/14 16:31
 */
public class CompareAndGetDiff extends GenericUDTF {
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        ArrayList<String> fieldName = new ArrayList<>();
        fieldName.add("event_name");
        fieldName.add("event_value");

        ArrayList<ObjectInspector> fieldType = new ArrayList<>();
        fieldType.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldType.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldName, fieldType);
    }

    @Override
    public void process(Object[] objects) throws HiveException {

    }

    @Override
    public void close() throws HiveException {

    }
}
