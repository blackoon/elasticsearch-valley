package com.hylanda.valley.search.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

/**
 * @author zhangy
 * @E-mail:blackoon88@gmail.com
 * @version 创建时间：2016年6月20日 下午3:10:53 note
 */
public class ValleyTransportAction extends
		TransportAction<ValleyRequest, ValleyResponse> {
	@Inject
	protected ValleyTransportAction(Settings settings, String actionName,
			ThreadPool threadPool, ActionFilters actionFilters) {
		super(settings, actionName, threadPool, actionFilters);
	}

	protected final ESLogger logger = Loggers.getLogger(getClass());

	@Override
	protected void doExecute(ValleyRequest request,
			ActionListener<ValleyResponse> listener) {

	}

}
