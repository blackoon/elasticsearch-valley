package com.hylanda.valley.search.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;

/** 
 * @author zhangy
 * @E-mail:blackoon88@gmail.com 
 * @version 创建时间：2016年6月20日 下午2:45:06 
 * note
 */
public class ValleyServer extends AbstractLifecycleComponent<ValleyServer>{
	
	protected final ESLogger logger= Loggers.getLogger(getClass());
	
	@Inject
	protected ValleyServer(Settings settings) {
		super(settings);
	}

	@Override
	protected void doStart() throws ElasticsearchException {
		
	}

	@Override
	protected void doStop() throws ElasticsearchException {
		
	}

	@Override
	protected void doClose() throws ElasticsearchException {
		// TODO Auto-generated method stub
		
	}

}
