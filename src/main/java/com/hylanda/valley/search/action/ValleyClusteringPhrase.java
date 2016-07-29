package com.hylanda.valley.search.action;

import java.util.Map;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.SearchContext;

/**
 * @author zhangy
 * @E-mail:blackoon88@gmail.com
 * @version 创建时间：2016年6月21日 上午9:27:51 note
 */
public class ValleyClusteringPhrase implements FetchSubPhase {
	protected final ESLogger logger = Loggers.getLogger(getClass());

	public Map<String, ? extends SearchParseElement> parseElements() {
		return ImmutableMap.of("clusters", new ValleyParseElement());
	}

	public boolean hitsExecutionNeeded(SearchContext context) {
		return false;
	}

	public void hitsExecute(SearchContext context, InternalSearchHit[] hits)
			throws ElasticsearchException {

	}

	public boolean hitExecutionNeeded(SearchContext context) {
		// todo
		return true;
		// return context.cluster() != null;
	}

	public void hitExecute(SearchContext context, HitContext hitContext)
			throws ElasticsearchException {
	}
}
