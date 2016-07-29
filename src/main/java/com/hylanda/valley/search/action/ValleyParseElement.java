package com.hylanda.valley.search.action;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.internal.SearchContext;

/** 
 * @author zhangy
 * @E-mail:blackoon88@gmail.com 
 * @version 创建时间：2016年6月21日 下午4:47:53 
 * note
 */
public class ValleyParseElement implements SearchParseElement{
	protected final ESLogger logger= Loggers.getLogger(getClass());
	
	public void parse(XContentParser parser, SearchContext context)
			throws Exception {
		
	}
}
