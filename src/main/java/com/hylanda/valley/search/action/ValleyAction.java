package com.hylanda.valley.search.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

/** 
 * @author zhangy
 * @E-mail:blackoon88@gmail.com 
 * @version 创建时间：2016年6月20日 下午2:12:29 
 * note
 */
public class ValleyAction extends Action<ValleyRequest, ValleyResponse, ValleyRequestBuilder,Client>{
	
	protected final ESLogger logger= Loggers.getLogger(getClass());
	public static final ValleyAction INSTANCE =new ValleyAction();
	public static final String NAME = "Valley";
	
	
	protected ValleyAction (){
		super(NAME);
	}
	
	protected ValleyAction(String name) {
		super(name);
	}

	@Override
	public ValleyRequestBuilder newRequestBuilder(Client client) {
		return new ValleyRequestBuilder(client, new ValleyRequest());
	}

	@Override
	public ValleyResponse newResponse() {
		return new ValleyResponse();
	}

}
