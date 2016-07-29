package com.hylanda.valley.search.action;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;

/**
 * @author zhangy
 * @E-mail:blackoon88@gmail.com
 * @version 创建时间：2016年6月20日 下午2:43:38 note
 */
public class ValleyServerModule extends AbstractModule {
	private final Settings settings;
	protected final ESLogger logger = Loggers.getLogger(getClass());

	public ValleyServerModule(Settings settings) {
		this.settings = settings;
	}

	@Override
	protected void configure() {
		bind(ValleyClusteringPhrase.class).asEagerSingleton();
	}

}
