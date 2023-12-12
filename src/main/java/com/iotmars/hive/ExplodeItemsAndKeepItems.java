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
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * 炸开items并保留items字段；因为运行太耗时，
 * 这里取需要的字段LStoveStatus、RStoveStatus、StOvState、StStatus/OvStatus、StreamStatus、LStOvState、RStOvState、HoodSpeed
 * 联动字段StOvMode、StMode/OvMode、SteamerMode、LStOvMode、RStOvMode
 *
 * @author CJ
 * @date: 2023/12/11 11:14
 */
public class ExplodeItemsAndKeepItems extends GenericUDTF {
    private List<String> extraFields;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        extraFields = Arrays.asList("LStoveStatus", "RStoveStatus", "StOvState", "StStatus", "OvStatus", "StreamStatus", "LStOvState", "RStOvState", "HoodSpeed");

        // 1.检查参数合法性
        if (argOIs.length != 1) {
            throw new UDFArgumentException("只需要一个参数");
        }

        // 2.第一个参数必须为string
        // 判断参数是否为基础数据类型
        if (argOIs[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentException("只接受基础类型参数");
        }
        // 将参数对象检查器强制转为基础类型对象检查器
        PrimitiveObjectInspector argument = (PrimitiveObjectInspector) argOIs[0];
        // 判断参数类型是否为string类型
        if (argument.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            throw new UDFArgumentException("只接受string类型参数");
        }

        // 3.定义返回值名称和类型
        ArrayList<String> fieldNames = new ArrayList<>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<>();

        fieldNames.add("event_time");
        fieldNames.add("event_name");
        fieldNames.add("event_value");
        fieldNames.add("items");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        Object arg = args[0];
        String jsonStr = PrimitiveObjectInspectorUtils.getString(arg, PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        // 解析json
        try {
            JSONObject jsonObject = new JSONObject(jsonStr);
            Set<String> keySet = jsonObject.keySet();

            for (String key : keySet) {
                try {
                    if (extraFields.contains(key)) {
                        JSONObject valueJson = jsonObject.getJSONObject(key);
                        String value = valueJson.getString("value");
                        String time = valueJson.getString("time");

                        // 寻找联动属性 StOvState、StStatus/OvStatus、StreamStatus、LStOvState、RStOvState
                        // 联动属性 StOvMode、StMode/OvMode、SteamerMode、LStOvMode、RStOvMode
                        JSONObject relaFieldJson = null;
                        String relaFieldValue = "";
                        try {
                            if ("LStOvState".equals(key)) {
                                relaFieldJson = jsonObject.getJSONObject("LStOvMode");
                            } else if ("RStOvState".equals(key)) {
                                relaFieldJson = jsonObject.getJSONObject("RStOvMode");
                            } else if ("StOvState".equals(key)) {
                                relaFieldJson = jsonObject.getJSONObject("StOvMode");
                            } else if ("StStatus".equals(key)) {
                                relaFieldJson = jsonObject.getJSONObject("StMode");
                            } else if ("OvStatus".equals(key)) {
                                relaFieldJson = jsonObject.getJSONObject("OvMode");
                            } else if ("StreamStatus".equals(key)) {
                                relaFieldJson = jsonObject.getJSONObject("SteamerMode");
                            }
                            if (relaFieldJson != null) {
                                relaFieldValue = relaFieldJson.getString("value");
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                        String[] result = {time, key, value, relaFieldValue};
                        forward(result);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws HiveException {

    }
}
