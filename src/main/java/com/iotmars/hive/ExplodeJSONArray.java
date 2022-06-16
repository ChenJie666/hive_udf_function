package com.iotmars.hive;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.json.JSONArray;

import java.util.ArrayList;
import java.util.logging.Logger;

/**
 * 用于解析edb的json数据
 * @author CJ
 * @date: 2021/11/2 9:44
 */
public class ExplodeJSONArray extends GenericUDTF {

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        // 1.检查参数合法性
        if (argOIs.length != 1) {
            throw new UDFArgumentException("只需要一个参数");
        }

        // 2. 第一个参数必须为string
        // 判断参数是否为基础数据类型
        if (argOIs[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentException("只接受基础类型参数");
        }

        // 将参数对象检查器强制转为基础类型对象检查其
        PrimitiveObjectInspector argument = (PrimitiveObjectInspector) argOIs[0];

        // 判断参数是否为String类型
        if (argument.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            throw new UDFArgumentException("只接受string类型参数");
        }

        // 定义返回值名称和类型
        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

        fieldNames.add("items");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] objects) throws HiveException {
        Object arg = objects[0];
        String jsonArrayStr = PrimitiveObjectInspectorUtils.getString(arg, PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        try {
            JSONArray jsonArray = new JSONArray(jsonArrayStr);

            for (int i = 0; i < jsonArray.length(); i++) {
                String json = jsonArray.getString(i);
                String[] result = {json};
                forward(result);
            }
        } catch (Exception e) {
            Logger.getLogger("ExplodeJSONArray").warning("FastJson解析失败: " + jsonArrayStr);
        }
    }

    @Override
    public void close() throws HiveException {

    }
}
