package com.vasileva.ua;


import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.PRIMITIVE;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.STRING;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaStringObjectInspector;

/**
 * Function parses device, operating system and browser from raw user agent.
 */
@UDFType
@Description(
        name = "parseUA",
        value = "_FUNC_(user_agent_string) - parses device, os and browser from user_agent_string",
        extended = "Example:\n" +
                " > SELECT parseUA('Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.11 (KHTML, like Gecko) " +
                "Chrome/17.0.963.84 Safari/535.11') FROM bids;\n" +
                "   Computer\tWindows 7\tChrome 17\n")
public class ParseUserAgentUDTF extends GenericUDTF {

    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        if (argOIs.getAllStructFieldRefs().size() != 1) {
            throw new UDFArgumentException("ParseUserAgentUDTF takes only one argument");
        }
        ObjectInspector arg = argOIs.getAllStructFieldRefs().get(0).getFieldObjectInspector();
        if (arg.getCategory() != PRIMITIVE || ((PrimitiveObjectInspector) arg).getPrimitiveCategory() != STRING) {
            throw new UDFArgumentException("ParseUserAgentUDTF takes only string argument");
        }

        List<String> fieldNames = Arrays.asList("device", "os", "browser");
        List<ObjectInspector> fieldInspectors = IntStream.rangeClosed(0, fieldNames.size())
                .mapToObj(idx -> javaStringObjectInspector)
                .collect(Collectors.toList());
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldInspectors);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        UserAgent ua = UserAgent.parseUserAgentString(javaStringObjectInspector.getPrimitiveJavaObject(args[0]));
        Object[] res = new Object[]{
                ua.getOperatingSystem().getDeviceType().getName(),
                ua.getOperatingSystem().getName(),
                ua.getBrowser().getName()
        };
        forward(res);

    }

    @Override
    public void close() {
    }
}
