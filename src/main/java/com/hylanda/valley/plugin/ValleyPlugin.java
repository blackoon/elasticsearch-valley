package com.hylanda.valley.plugin;

import static org.elasticsearch.common.collect.Lists.newArrayList;

import java.util.Collection;

import org.elasticsearch.action.ActionModule;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.rest.RestModule;
import org.elasticsearch.search.aggregations.AggregationModule;

import com.hylanda.valley.cluster.ValleyClusteringParser;
import com.hylanda.valley.output.InternalValleyClustering;
import com.hylanda.valley.search.action.ValleyAction;
import com.hylanda.valley.search.action.ValleyRestAction;
import com.hylanda.valley.search.action.ValleyServer;
import com.hylanda.valley.search.action.ValleyServerModule;
import com.hylanda.valley.search.action.ValleyTransportAction;

/**
 * @author zhangy
 * @E-mail:blackoon88@gmail.com
 * @version 创建时间：2016年6月20日 下午2:12:29 note
 */

public class ValleyPlugin extends AbstractPlugin {

	protected final ESLogger logger = Loggers.getLogger(getClass());

//	private final Settings settings;

//	@Inject
//	public ValleyPlugin(Settings settings) {
//		this.settings = settings;
//	}

	public String name() {
		return "valley";
	}

	public String description() {
		return "valley is used for querying logic on the server-side";
	}

//	public void onModule(ActionModule module) {
//		module.registerAction(ValleyAction.INSTANCE,
//				ValleyTransportAction.class);
//	}
//
//	public void onModule(RestModule module) {
//		module.addRestAction(ValleyRestAction.class);
//	}

	public void onModule(AggregationModule module) {
		module.addAggregatorParser(ValleyClusteringParser.class);
		InternalValleyClustering.registerStreams();
	}

//	@Override
//	public Collection<Class<? extends Module>> modules() {
//		Collection<Class<? extends Module>> modules = newArrayList();
//		if (settings.getAsBoolean("valley.enabled", true)) {
//			modules.add(ValleyServerModule.class);
//		}
//		return super.modules();
//	}
//
//	@SuppressWarnings("rawtypes")
//	@Override
//	public Collection<Class<? extends LifecycleComponent>> services() {
//		Collection<Class<? extends LifecycleComponent>> services = newArrayList();
//		if (settings.getAsBoolean("valley.enabled", true)) {
//			services.add(ValleyServer.class);
//		}
//		return services;
//	}
}
