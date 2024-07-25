package com.iotmars.hive;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * 炸开items并保留items字段；因为运行太耗时,这里取需要的字段LStoveStatus、RStoveStatus、StOvState、StStatus/OvStatus、StreamStatus、LStOvState、RStOvState、HoodSpeed
 * 联动字段StOvMode、StMode/OvMode、SteamerMode、LStOvMode、RStOvMode
 *
 * @author CJ
 * @date: 2023/12/11 11:14
 */
public class ExplodeItemsAndKeepItemsV2 extends GenericUDTF {
    private static final Logger logger = LoggerFactory.getLogger(ExplodeItemsAndKeepItemsV2.class);

    //    private final List<String> extraFields = Arrays.asList("ErrorCode","LStoveStatus", "RStoveStatus", "StOvState", "StStatus", "OvStatus", "StreamStatus", "LStOvState", "RStOvState", "HoodSpeed"
//            , "SmartSmokeValid", "HoodStoveLink", "HoodLightLink", "OilTempSwitch", "HoodOffTimer", "LMovePotLowHeatSwitch", "RMovePotLowHeatSwitch"
//            , "CookingCurveSwitch", "LStoveTimingState", "RStoveTimingState", "LAuxiliarySwitch", "RAuxiliarySwitch");
    // 智能功能相关字段，如烟机延时，
    private final List<String> extraSmartFunctionFields = Arrays.asList("HoodStoveLink", "HoodOffTimer", "OilTempSwitch", "LMovePotLowHeatSwitch", "RMovePotLowHeatSwitch", "HoodLightLink", "CookingCurveSwitch");

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {

        // 1.检查参数合法性
        if (argOIs.length != 3) {
            throw new UDFArgumentException("需要三个参数，分别是items(String)、需要筛选出的字段(Array)和关联字段(Array)!");
        }

        // 2.第一个参数必须为string
        // 判断参数是否为基础数据类型
        if (argOIs[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentException("只接受String类型参数");
        }
        if (argOIs[1].getCategory() != ObjectInspector.Category.LIST) {
            throw new UDFArgumentException("只接受Array类型参数");
        }
        if (argOIs[2].getCategory() != ObjectInspector.Category.LIST) {
            throw new UDFArgumentException("只接受Array类型参数");
        }
        // 将参数对象检查器强制转为基础类型对象检查器
        PrimitiveObjectInspector argument0 = (PrimitiveObjectInspector) argOIs[0];
        ListObjectInspector argument1 = (ListObjectInspector) argOIs[1];
        ListObjectInspector argument2 = (ListObjectInspector) argOIs[2];
        // 判断参数类型是否为string类型
        if (argument0.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            throw new UDFArgumentException("只接受String类型参数");
        }
        if (argument1.getCategory() != ObjectInspector.Category.LIST) {
            throw new UDFArgumentException("只接受Array类型参数");
        }
        if (argument2.getCategory() != ObjectInspector.Category.LIST) {
            throw new UDFArgumentException("只接受Array类型参数");
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
        Object arg0 = args[0];
        String itemJson = PrimitiveObjectInspectorUtils.getString(arg0, PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        List<Text> extraFields = (List<Text>) args[1];
        List<Text> linkFields = (List<Text>) args[2];

        // 解析json
        try {
            JSONObject jsonObject = JSON.parseObject(itemJson);
            Set<String> keySet = jsonObject.keySet();

            if (!keySet.contains("action")) {
                for (String key : keySet) {
                    try {
                        if (extraFields.stream().anyMatch(text -> text.toString().equalsIgnoreCase(key))) {
                            JSONObject valueJson = jsonObject.getJSONObject(key);
                            String value = valueJson.getString("value");
                            String time = valueJson.getString("time");

                            if (value != null) {
                                // 如果数据存在，则寻找联动属性
                                JSONObject relaFieldJson = new JSONObject();
                                linkFields.forEach(field -> {
                                    Optional<String> fieldValueOptional = Optional.ofNullable(jsonObject.getJSONObject(field.toString())).map(jo -> jo.getString("value"));
                                    fieldValueOptional.ifPresent(s -> relaFieldJson.put(field.toString(), s));
                                });

                                // 写出
                                String[] result = {time, key, value, relaFieldJson.toJSONString()};
                                forward(result);
                            }
                        }
                    } catch (Exception e) {
                        logger.error("Error", e);
                    }
                }
            }
//            else {
//                // 如果是上下线信息，则插入value为0的记录
//                JSONObject valueJson = jsonObject.getJSONObject("status");
//                if (valueJson != null) {
//                    String time = valueJson.getString("time");
//                    for (Text extraField : extraFields) {
//                        // 智能功能的event_name需要排除掉，因为取最新的记录来判断该设备是否开启的该功能
//                        if (!extraSmartFunctionFields.contains(extraField.toString())) {
//                            String[] result = {time, extraField.toString(), "0", null};
//                            forward(result);
//                        }
//                    }
//                }
//            }
        } catch (Exception e) {
            logger.error("Error", e);
        }
    }

    @Override
    public void close() throws HiveException {

    }
}