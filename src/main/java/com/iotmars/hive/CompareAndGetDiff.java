package com.iotmars.hive;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Objects;
import java.util.Set;
import java.util.logging.Logger;

/**
 * 用于飞燕数据，比较两个json串获取差异的属性
 *
 * @author CJ
 * @date: 2022/6/14 16:31
 */
public class CompareAndGetDiff extends GenericUDTF {

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        // 1. 检查参数合法性
        if (argOIs.length != 2) {
            throw new UDFArgumentException("需要两个参数");
        }

        // 2. 参数必须都为string
        if (argOIs[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentException("只接受基础类型参数");
        }

        PrimitiveObjectInspector argument0 = (PrimitiveObjectInspector) argOIs[0];
        PrimitiveObjectInspector argument1 = (PrimitiveObjectInspector) argOIs[1];

        if (argument0.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            throw new UDFArgumentException("只接受string类型");
        }

        // 2. 定义返回值名称和类型
        ArrayList<String> fieldNames = new ArrayList<>();
        fieldNames.add("event_name");
        fieldNames.add("event_value");
        fieldNames.add("event_ori_value");

        ArrayList<ObjectInspector> fieldType = new ArrayList<>();
        fieldType.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldType.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldType.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldType);
    }

    @Override
    public void process(Object[] objects) throws HiveException {
        // 检查参数
        if (Objects.isNull(objects[0]) || Objects.isNull(objects[1])) {
            return;
        }
        String arg0 = PrimitiveObjectInspectorUtils.getString(objects[0], PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        String arg1 = PrimitiveObjectInspectorUtils.getString(objects[1], PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        if (StringUtils.isEmpty(arg0) || StringUtils.isEmpty(arg1)) {
            return;
        }

        // 比较并获取改变的属性
        try {
            JSONObject newData = new JSONObject(arg0);
            JSONObject oldData = new JSONObject(arg1);

            Set<String> keys = newData.keySet();

            for (String key : keys) {
                String newValue = newData.getJSONObject(key).getString("value");
                String oldValue = oldData.getJSONObject(key).getString("value");

                Logger.getLogger("CompareAndGetDiff").warning("值: " + newValue + "  " + oldValue);
                if (!Objects.isNull(newValue) && !newValue.equalsIgnoreCase(oldValue)) {
                    String[] result = {key,newValue,oldValue};
                    forward(result);
                }
            }

        } catch (Exception e) {
            Logger.getLogger("CompareAndGetDiff").warning("FastJson解析失败: " + arg0 + "  " + arg1);
        }

    }

    @Override
    public void close() throws HiveException {

    }
}
