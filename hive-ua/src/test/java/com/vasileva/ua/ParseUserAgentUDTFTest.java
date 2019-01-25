package com.vasileva.ua;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.io.parquet.serde.ArrayWritableObjectInspector;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.Collector;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvFileSource;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ParseUserAgentUDTFTest {

    private static final GenericUDTF parseUA = new ParseUserAgentUDTF();

    @BeforeAll
    static void setUp() throws UDFArgumentException {
        StructTypeInfo info = new StructTypeInfo();
        info.setAllStructFieldNames(new ArrayList<>(Collections.singletonList("user_agent")));
        info.setAllStructFieldTypeInfos(new ArrayList<>(Collections.singletonList(TypeInfoFactory.stringTypeInfo)));
        StructObjectInspector inspector = new ArrayWritableObjectInspector(info);
        parseUA.initialize(inspector);
    }

    @Test
    void testTooManyArguments() {
        StructTypeInfo info = new StructTypeInfo();
        info.setAllStructFieldNames(new ArrayList<>(Arrays.asList("user_agent1", "userAgent2")));
        info.setAllStructFieldTypeInfos(new ArrayList<>(Arrays.asList(TypeInfoFactory.stringTypeInfo, TypeInfoFactory.stringTypeInfo)));
        StructObjectInspector inspector = new ArrayWritableObjectInspector(info);
        assertThrows(UDFArgumentException.class, () -> parseUA.initialize(inspector), "ParseUserAgentUDTF takes only one argument");
    }

    @Test
    void testInvalidArgument() {
        StructTypeInfo info = new StructTypeInfo();
        info.setAllStructFieldNames(new ArrayList<>(Collections.singletonList("user_agent")));
        info.setAllStructFieldTypeInfos(new ArrayList<>(Collections.singletonList(TypeInfoFactory.binaryTypeInfo)));
        StructObjectInspector inspector = new ArrayWritableObjectInspector(info);
        assertThrows(UDFArgumentException.class, () -> parseUA.initialize(inspector), "ParseUserAgentUDTF takes only string argument");
    }

    @ParameterizedTest
    @CsvFileSource(resources = "/user_agents.csv")
    void testParse(String userAgent, String expectedDevice, String expectedBrowser, String expectedOS) throws HiveException {
        List<UserAgentDetails> results = new ArrayList<>();
        Collector collector = data -> results.add(new UserAgentDetails((Object[]) data));
        parseUA.setCollector(collector);
        parseUA.process(new Object[]{userAgent});

        assertEquals(expectedDevice, results.get(0).device);
        assertEquals(expectedBrowser, results.get(0).browser);
        assertEquals(expectedOS, results.get(0).os);
    }

    @Test
    void testInvalidUserAgent() throws HiveException {
        List<UserAgentDetails> results = new ArrayList<>();
        Collector collector = data -> results.add(new UserAgentDetails((Object[]) data));
        parseUA.setCollector(collector);
        parseUA.process(new Object[]{null});

        assertEquals("Unknown", results.get(0).device);
        assertEquals("Unknown", results.get(0).device);
        assertEquals("Unknown", results.get(0).device);

        parseUA.process(new Object[]{""});

        assertEquals("Unknown", results.get(1).device);
        assertEquals("Unknown", results.get(1).device);
        assertEquals("Unknown", results.get(1).device);

        parseUA.process(new Object[]{"null"});

        assertEquals("Unknown", results.get(2).device);
        assertEquals("Unknown", results.get(2).device);
        assertEquals("Unknown", results.get(2).device);
    }

    private class UserAgentDetails {
        String device;
        String os;
        String browser;

        UserAgentDetails(Object[] values) {
            this.device = values[0].toString();
            this.os = values[1].toString();
            this.browser = values[2].toString();
        }
    }
}
