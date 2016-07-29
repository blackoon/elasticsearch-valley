package com.hylanda.valley.search.action;

import org.carrot2.clustering.lingo.LingoClusteringAlgorithm;
import org.carrot2.core.Controller;
import org.carrot2.core.ControllerFactory;
import org.carrot2.core.IDocumentSource;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;

/** 
 * @author zhangy
 * @E-mail:blackoon88@gmail.com 
 * @version 创建时间：2016年6月20日 下午2:53:40 
 * note
 */
public class ValleyRestAction extends BaseRestHandler{

	final Controller controller = ControllerFactory.createCachingPooling(IDocumentSource.class,LingoClusteringAlgorithm.class);
	Environment environment;
	@Inject
	protected ValleyRestAction(Settings settings, RestController restController,
			Client client) {
		super(settings, restController, client);
		environment=new Environment(settings);
		restController.registerHandler(RestRequest.Method.POST, "/{index}/{type}/_Valley", this);
        restController.registerHandler(RestRequest.Method.POST, "/{index}/{type}/_Valley/{algorithm}", this);
        restController.registerHandler(RestRequest.Method.POST, "/{index}/_Valley", this);
        restController.registerHandler(RestRequest.Method.GET, "/{index}/_Valley", this);
        restController.registerHandler(RestRequest.Method.POST, "/{index}/_Valley/{algorithm}", this);
        restController.registerHandler(RestRequest.Method.POST, "/_Valley/{algorithm}", this);
        restController.registerHandler(RestRequest.Method.POST, "/_Valley", this);
        restController.registerHandler(RestRequest.Method.GET, "/_Valley", this);

        restController.registerHandler(RestRequest.Method.POST, "/{index}/{type}/_search_clustering", this);
        restController.registerHandler(RestRequest.Method.POST, "/{index}/{type}/_search_clustering/{algorithm}", this);
        restController.registerHandler(RestRequest.Method.POST, "/{index}/_search_clustering", this);
        restController.registerHandler(RestRequest.Method.POST, "/{index}/_search_clustering/{algorithm}", this);
        restController.registerHandler(RestRequest.Method.POST, "/_search_clustering/{algorithm}", this);
        restController.registerHandler(RestRequest.Method.POST, "/_search_clustering", this);
	}
	

	@Override
	protected void handleRequest(RestRequest request, RestChannel channel,
			Client client) throws Exception {

	}

	    private SearchSourceBuilder parseSearchSource(RestRequest request) {
	        SearchSourceBuilder searchSourceBuilder = null;
	        String queryString = request.param("q");
	        if (queryString != null) {
	            QueryStringQueryBuilder queryBuilder = QueryBuilders.queryString(queryString);
	            queryBuilder.defaultField(request.param("df"));
	            queryBuilder.analyzer(request.param("analyzer"));
	            queryBuilder.analyzeWildcard(request.paramAsBoolean("analyze_wildcard", false));
	            queryBuilder.lowercaseExpandedTerms(request.paramAsBoolean("lowercase_expanded_terms", true));
	            String defaultOperator = request.param("default_operator");
	            if (defaultOperator != null) {
	                if ("OR".equals(defaultOperator)) {
	                    queryBuilder.defaultOperator(QueryStringQueryBuilder.Operator.OR);
	                } else if ("AND".equals(defaultOperator)) {
	                    queryBuilder.defaultOperator(QueryStringQueryBuilder.Operator.AND);
	                } else {
	                    throw new ElasticsearchIllegalArgumentException("Unsupported defaultOperator [" + defaultOperator + "], can either be [OR] or [AND]");
	                }
	            }
	            if (searchSourceBuilder == null) {
	                searchSourceBuilder = new SearchSourceBuilder();
	            }
	            searchSourceBuilder.query(queryBuilder);
	        }

	        int from = request.paramAsInt("from", -1);
	        if (from != -1) {
	            if (searchSourceBuilder == null) {
	                searchSourceBuilder = new SearchSourceBuilder();
	            }
	            searchSourceBuilder.from(from);
	        }
	        int size = request.paramAsInt("size", -1);
	        if (size != -1) {
	            if (searchSourceBuilder == null) {
	                searchSourceBuilder = new SearchSourceBuilder();
	            }
	            searchSourceBuilder.size(size);
	        }

	        if (request.hasParam("explain")) {
	            if (searchSourceBuilder == null) {
	                searchSourceBuilder = new SearchSourceBuilder();
	            }
	            searchSourceBuilder.explain(request.paramAsBooleanOptional("explain", null));
	        }
	        if (request.hasParam("version")) {
	            if (searchSourceBuilder == null) {
	                searchSourceBuilder = new SearchSourceBuilder();
	            }
	            searchSourceBuilder.version(request.paramAsBooleanOptional("version", null));
	        }
	        if (request.hasParam("timeout")) {
	            if (searchSourceBuilder == null) {
	                searchSourceBuilder = new SearchSourceBuilder();
	            }
	            searchSourceBuilder.timeout(request.paramAsTime("timeout", null));
	        }

	        String sField = request.param("fields");
	        if (sField != null) {
	            if (searchSourceBuilder == null) {
	                searchSourceBuilder = new SearchSourceBuilder();
	            }
	            if (!Strings.hasText(sField)) {
	                searchSourceBuilder.noFields();
	            } else {
	                String[] sFields = Strings.splitStringByCommaToArray(sField);
	                if (sFields != null) {
	                    for (String field : sFields) {
	                        searchSourceBuilder.field(field);
	                    }
	                }
	            }
	        }

	        String sSorts = request.param("sort");
	        if (sSorts != null) {
	            if (searchSourceBuilder == null) {
	                searchSourceBuilder = new SearchSourceBuilder();
	            }
	            String[] sorts = Strings.splitStringByCommaToArray(sSorts);
	            for (String sort : sorts) {
	                int delimiter = sort.lastIndexOf(":");
	                if (delimiter != -1) {
	                    String sortField = sort.substring(0, delimiter);
	                    String reverse = sort.substring(delimiter + 1);
	                    if ("asc".equals(reverse)) {
	                        searchSourceBuilder.sort(sortField, SortOrder.ASC);
	                    } else if ("desc".equals(reverse)) {
	                        searchSourceBuilder.sort(sortField, SortOrder.DESC);
	                    }
	                } else {
	                    searchSourceBuilder.sort(sort);
	                }
	            }
	        }

	        String sIndicesBoost = request.param("indices_boost");
	        if (sIndicesBoost != null) {
	            if (searchSourceBuilder == null) {
	                searchSourceBuilder = new SearchSourceBuilder();
	            }
	            String[] indicesBoost = Strings.splitStringByCommaToArray(sIndicesBoost);
	            for (String indexBoost : indicesBoost) {
	                int divisor = indexBoost.indexOf(',');
	                if (divisor == -1) {
	                    throw new ElasticsearchIllegalArgumentException("Illegal index boost [" + indexBoost + "], no ','");
	                }
	                String indexName = indexBoost.substring(0, divisor);
	                String sBoost = indexBoost.substring(divisor + 1);
	                try {
	                    searchSourceBuilder.indexBoost(indexName, Float.parseFloat(sBoost));
	                } catch (NumberFormatException e) {
	                    throw new ElasticsearchIllegalArgumentException("Illegal index boost [" + indexBoost + "], boost not a float number");
	                }
	            }
	        }

	        String sStats = request.param("stats");
	        if (sStats != null) {
	            if (searchSourceBuilder == null) {
	                searchSourceBuilder = new SearchSourceBuilder();
	            }
	            searchSourceBuilder.stats(Strings.splitStringByCommaToArray(sStats));
	        }

	        return searchSourceBuilder;
	    }
}
