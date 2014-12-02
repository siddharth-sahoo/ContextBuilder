package com.example;

import java.util.HashMap;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.awesome.pro.context.ContextBuilder;
import com.awesome.pro.context.ContextFactory;
import com.awesome.pro.db.cassandra.client.CassandraClientManager;

public class Test2 {

	private static final String CONTEXT_NAME = "SampleContext";
	private static final String NAME_SPACE = "SampleNameSpace";
	
	private static ContextBuilder contextBuilder;
	
	@BeforeClass
	public static void init() {
		CassandraClientManager.initialize("cassandra.properties");
		contextBuilder = ContextFactory.getContextBuilder(CONTEXT_NAME);
		contextBuilder.addContextNameSpace(NAME_SPACE);
	}

	@Test
	public void test1() {
		final Map<String, String> values = new HashMap<>();
		values.put("field1", "val1");
		contextBuilder.addContextualData(NAME_SPACE, "key1", values);
		Assert.assertEquals(values, contextBuilder.getContextData(NAME_SPACE, "key1"));
	}

	@Test
	public void test2() {
		final Map<String, String> values = new HashMap<>();
		values.put("field1", "val2");
		contextBuilder.addContextualData(NAME_SPACE, "key1", values);
		Assert.assertEquals(values, contextBuilder.getContextData(NAME_SPACE, "key1"));
	}

	@AfterClass
	public static void tearDown() {
		try {
			contextBuilder.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		CassandraClientManager.shutdown();
	}

}
