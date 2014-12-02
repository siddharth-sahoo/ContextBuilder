package com.example;

import java.util.HashMap;
import java.util.Map;

import com.awesome.pro.context.CassandraContext;
import com.awesome.pro.context.ContextBuilder;

public class Test1 {
	
	public static void main(String[] args) {
		ContextBuilder context = new CassandraContext("SampleContext");
		
		context.addContextNameSpace("sample_CS1"); 
		context.addContextNameSpace("sample_CS2"); 
		context.addContextNameSpace("sample_CS3"); 
		Map<String, String> fieldValMap = new HashMap<String, String>();
		fieldValMap.put("col_1", "val_1");
		fieldValMap.put("col_2", "val_2");
		fieldValMap.put("col_3", "val_3");
		fieldValMap.put("col_4", "val_4");
		context.addContextualData("sample_CS1", "sample_RK1", fieldValMap);
		context.addContextualData("sample_CS1", "sample_RK2", fieldValMap);
		context.addContextualData("sample_CS2", "sample_RK1", fieldValMap);
		//ColumnList<String> colList1 = CassandraUtilities.queryRow("sample_KS1", "sample_CS1", "sample_RK");
		//ColumnList<String> colList2 = CassandraUtilities.queryRow("sample_KS1", "sample_CS1", "sample_RK");
		//System.out.println(colList1.size());
		//System.out.println(colList2.size());
		
		//System.out.println(colList1.getStringValue("col_3", ""));
		//System.out.println(context.getNamespaces());
		
		
		Map<String, Map<String, String>> contextData = context.getContextData("sample_CS1");
		System.out.println("contextData _ namespace "+ contextData);
		
		Map<String, String> contextData_1 = context.getContextData("sample_CS1", "sample_RK1");
		System.out.println("contextData _ namespace-key "+contextData_1.toString());
		
		contextData_1 = context.getContextData("sample_CS1", "sample_RK2");
		System.out.println("contextData _ namespace-key "+contextData_1.toString());
		
		String contextData_2 = context.getContextData("sample_CS1", "sample_RK1", "col_3");
		System.out.println("contextData _ namespace-key-fied "+contextData_2);
		
		Map<String, String> contextDataByField = context.getContextDataByField("sample_CS1", "col_1");
		System.out.println(contextDataByField);
		
		try {
			context.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
