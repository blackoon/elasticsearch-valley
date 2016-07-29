package com.hylanda.valley.cluster;

import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.index.AtomicReaderContext;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSource.Bytes;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

/** 
 * @author zhangy
 * @E-mail:blackoon88@gmail.com 
 * @qq:846579287
 * @version 创建时间：2016年1月6日 下午4:11:02 
 * note
 */
public class ChildFields {
	
	Map<String, Field> fields = new HashMap<>();
	
	public static class Field {
		String name;
		ValuesSourceConfig<Bytes> fieldConfig;
		ValuesSource.Bytes		 fieldBytes;
		SortedBinaryDocValues	 fieldValues;
	}
	
	public void addFiled(String name , ValuesSourceConfig<Bytes> fieldConfig) {
		Field f = new Field();
		f.name = name;
		f.fieldConfig = fieldConfig;
		fields.put(name, f);
	}

	public void apply(AggregationContext aggregationContext, int depth) {
		for(Field field : fields.values()) {
			field.fieldBytes =  aggregationContext.valuesSource(field.fieldConfig, depth);
		}
		
	}

	public void setNextReader(AtomicReaderContext reader) {
		for(Field field : fields.values()) {
			field.fieldValues = field.fieldBytes.bytesValues();
		}
	}

	public void setDocument(int docId) {
		for(Field field : fields.values()) {
			field.fieldValues.setDocument(docId);
		}
	}
	public Map<String, Field> getFields() {
		return fields;
	}
}
