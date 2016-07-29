package com.hylanda.valley.search.action;

import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestController;

/** 
 * @author zhangy
 * @E-mail:blackoon88@gmail.com 
 * @version 创建时间：2016年6月21日 下午4:57:39 
 * note
 */
public class ValleyRestImpl extends AbstractComponent {
	protected final ESLogger logger= Loggers.getLogger(getClass());
	
	@SuppressWarnings("unused")
	private final RestController restController;
	
	@Inject
	public ValleyRestImpl(Settings settings, RestController restController) {
		super(settings);
		this.restController = restController;
	}

}
