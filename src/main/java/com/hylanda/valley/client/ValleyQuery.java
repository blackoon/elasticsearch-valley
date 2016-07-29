package com.hylanda.valley.client;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

public class ValleyQuery {
	@Inject
	private Client client;
	
	public SearchResponse query(SearchRequest request) throws IOException {
		return client.search(request).actionGet();
	}
	
	public static XContentBuilder maketermQuery(String field, String value) throws IOException {
		XContentBuilder query = XContentFactory.jsonBuilder().startObject();
		query.startObject("term");
		query.field(field, value);
		
		query.endObject();
		return query;
	}

	public SearchResponse queryByURLCrc(String urlCrc ,String[] indices) throws IOException {
		SearchSourceBuilder builder = new SearchSourceBuilder();
		builder.query(ValleyQuery.maketermQuery("url_crc", urlCrc));
		builder.size(1);
		builder.field("url_crc");
		
		SearchRequest req = new SearchRequest();
		req.indices(indices);
		req.types(indices);
		req.source(builder);
		
		SearchResponse res = query(req);
		SearchHits hits = res.getHits();
		if(hits != null && hits.getTotalHits() >0) {
			List<Object> md5s = hits.getHits()[0].field("md5s").getValues();
			return queryByMd5s(md5s, 3, 1000,indices);
		}
		return null;
	}
	
	private SearchResponse queryByMd5s(List<Object> md5s, int i, int j,String[] indices) {
		SearchRequest request=new SearchRequest(indices);
//		request.s
		client.search(request).actionGet();
		return null;
	}

	public String toJson(SearchResponse res) throws IOException {
		if(res == null) {
			return "[]";
		}
		XContentBuilder query = XContentFactory.jsonBuilder().startObject();
		query.field("timer", res.getTookInMillis());
		query.startArray("list");
		
		SearchHits hits = res.getHits();
		for(SearchHit item :  hits.getHits()) {
			Map<String, Object> source = item.getSource();
			query.startObject();
			query.field("author", source.get("author"));
			query.field("url", source.get("url"));
			query.field("title", source.get("title"));
			query.field("release_date", source.get("release_date"));
			query.field("media_name", source.get("media_name"));
			query.field("content_media_name", source.get("content_media_name"));
			query.endObject();
		}
		query.endArray();
		query.endObject();
		return query.string();
	}
}
