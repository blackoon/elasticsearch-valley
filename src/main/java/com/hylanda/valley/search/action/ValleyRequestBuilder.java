package com.hylanda.valley.search.action;

import java.util.Map;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.facet.FacetBuilder;
import org.elasticsearch.search.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortOrder;

/** 
 * @author zhangy
 * @E-mail:blackoon88@gmail.com 
 * @version 创建时间：2016年6月20日 下午2:20:38 
 * note
 */
public class ValleyRequestBuilder extends ActionRequestBuilder<ValleyRequest, ValleyResponse, ValleyRequestBuilder, Client>{
	
	protected final ESLogger logger= Loggers.getLogger(getClass());
	
	private ValleySourceBuilder sourceBuilder;
	
	@Override
    public ListenableActionFuture<ValleyResponse> execute() {
        return super.execute();
    }
	
	protected ValleyRequestBuilder(Client client, ValleyRequest request) {
		super(client, request);
	}

	

	@Override
	protected void doExecute(ActionListener<ValleyResponse> listener) {
		super.execute(listener);
	}

	 /**
     * Sets the indices the search will be executed on.
     */
    public ValleyRequestBuilder setIndices(String... indices) {
        request.indices(indices);
        return this;
    }

    /**
     * The document types to execute the search against. Defaults to be executed against
     * all types.
     */
    public ValleyRequestBuilder setTypes(String... types) {
        request.types(types);
        return this;
    }

    /**
     * The search type to execute, defaults to {@link org.elasticsearch.action.search.SearchType#DEFAULT}.
     */
    public ValleyRequestBuilder setSearchType(SearchType searchType) {
        request.searchType(searchType);
        return this;
    }

    /**
     * The a string representation search type to execute, defaults to {@link SearchType#DEFAULT}. Can be
     * one of "dfs_query_then_fetch"/"dfsQueryThenFetch", "dfs_query_and_fetch"/"dfsQueryAndFetch",
     * "query_then_fetch"/"queryThenFetch", and "query_and_fetch"/"queryAndFetch".
     */
    public ValleyRequestBuilder setSearchType(String searchType) throws ElasticsearchIllegalArgumentException {
        request.searchType(searchType);
        return this;
    }

    /**
     * If set, will enable scrolling of the search request.
     */
    public ValleyRequestBuilder setScroll(Scroll scroll) {
        request.scroll(scroll);
        return this;
    }

    /**
     * If set, will enable scrolling of the search request for the specified timeout.
     */
    public ValleyRequestBuilder setScroll(TimeValue keepAlive) {
        request.scroll(keepAlive);
        return this;
    }

    /**
     * If set, will enable scrolling of the search request for the specified timeout.
     */
    public ValleyRequestBuilder setScroll(String keepAlive) {
        request.scroll(keepAlive);
        return this;
    }

    /**
     * An optional timeout to control how long search is allowed to take.
     */
    public ValleyRequestBuilder setTimeout(TimeValue timeout) {
        sourceBuilder().timeout(timeout);
        return this;
    }

    /**
     * An optional timeout to control how long search is allowed to take.
     */
    public ValleyRequestBuilder setTimeout(String timeout) {
        sourceBuilder().timeout(timeout);
        return this;
    }

    /**
     * A comma separated list of routing values to control the shards the search will be executed on.
     */
    public ValleyRequestBuilder setRouting(String routing) {
        request.routing(routing);
        return this;
    }

    /**
     * The routing values to control the shards that the search will be executed on.
     */
    public ValleyRequestBuilder setRouting(String... routing) {
        request.routing(routing);
        return this;
    }

    /**
     * Sets the preference to execute the search. Defaults to randomize across shards. Can be set to
     * <tt>_local</tt> to prefer local shards, <tt>_primary</tt> to execute only on primary shards, or
     * a custom value, which guarantees that the same order will be used across different requests.
     */
    public ValleyRequestBuilder setPreference(String preference) {
        request.preference(preference);
        return this;
    }

    /**
     * Constructs a new search source builder with a search query.
     *
     * @see org.elasticsearch.index.query.QueryBuilders
     */
    public ValleyRequestBuilder setQuery(QueryBuilder queryBuilder) {
        sourceBuilder().query(queryBuilder);
        return this;
    }

    /**
     * Constructs a new search source builder with a raw search query.
     */
    public ValleyRequestBuilder setQuery(String query) {
        sourceBuilder().query(query);
        return this;
    }

    /**
     * Constructs a new search source builder with a raw search query.
     */
    public ValleyRequestBuilder setQuery(byte[] queryBinary) {
        sourceBuilder().query(queryBinary);
        return this;
    }

    /**
     * Constructs a new search source builder with a raw search query.
     */
    public ValleyRequestBuilder setQuery(byte[] queryBinary, int queryBinaryOffset, int queryBinaryLength) {
        sourceBuilder().query(queryBinary, queryBinaryOffset, queryBinaryLength);
        return this;
    }

    /**
     * Constructs a new search source builder with a raw search query.
     */
    public ValleyRequestBuilder setQuery(XContentBuilder query) {
        sourceBuilder().query(query);
        return this;
    }

    /**
     * Constructs a new search source builder with a raw search query.
     */
    public ValleyRequestBuilder setQuery(Map query) {
        sourceBuilder().query(query);
        return this;
    }

    /**
     * Sets the minimum score below which docs will be filtered out.
     */
    public ValleyRequestBuilder setMinScore(float minScore) {
        sourceBuilder().minScore(minScore);
        return this;
    }

    /**
     * From index to start the search from. Defaults to <tt>0</tt>.
     */
    public ValleyRequestBuilder setFrom(int from) {
        sourceBuilder().from(from);
        return this;
    }

    /**
     * The number of search hits to return. Defaults to <tt>10</tt>.
     */
    public ValleyRequestBuilder setSize(int size) {
        sourceBuilder().size(size);
        return this;
    }

    /**
     * Should each {@link org.elasticsearch.search.SearchHit} be returned with an
     * explanation of the hit (ranking).
     */
    public ValleyRequestBuilder setExplain(boolean explain) {
        sourceBuilder().explain(explain);
        return this;
    }

    /**
     * Should each {@link org.elasticsearch.search.SearchHit} be returned with its
     * version.
     */
    public ValleyRequestBuilder setVersion(boolean version) {
        sourceBuilder().version(version);
        return this;
    }

    /**
     * Sets the boost a specific index will receive when the query is executeed against it.
     *
     * @param index      The index to apply the boost against
     * @param indexBoost The boost to apply to the index
     */
    public ValleyRequestBuilder addIndexBoost(String index, float indexBoost) {
        sourceBuilder().indexBoost(index, indexBoost);
        return this;
    }

    /**
     * The stats groups this request will be aggregated under.
     */
    public ValleyRequestBuilder setStats(String... statsGroups) {
        sourceBuilder().stats(statsGroups);
        return this;
    }

    /**
     * Sets no fields to be loaded, resulting in only id and type to be returned per field.
     */
    public ValleyRequestBuilder setNoFields() {
        sourceBuilder().noFields();
        return this;
    }

    /**
     * Adds a field to load and return (note, it must be stored) as part of the search request.
     * If none are specified, the source of the document will be return.
     */
    public ValleyRequestBuilder addField(String field) {
        sourceBuilder().field(field);
        return this;
    }

    /**
     * Adds a script based field to load and return. The field does not have to be stored,
     * but its recommended to use non analyzed or numeric fields.
     *
     * @param name   The name that will represent this value in the return hit
     * @param script The script to use
     */
    public ValleyRequestBuilder addScriptField(String name, String script) {
        sourceBuilder().scriptField(name, script);
        return this;
    }

    /**
     * Adds a script based field to load and return. The field does not have to be stored,
     * but its recommended to use non analyzed or numeric fields.
     *
     * @param name   The name that will represent this value in the return hit
     * @param script The script to use
     * @param params Parameters that the script can use.
     */
    public ValleyRequestBuilder addScriptField(String name, String script, Map<String, Object> params) {
        sourceBuilder().scriptField(name, script, params);
        return this;
    }

    /**
     * Adds a script based field to load and return. The field does not have to be stored,
     * but its recommended to use non analyzed or numeric fields.
     *
     * @param name   The name that will represent this value in the return hit
     * @param lang   The language of the script
     * @param script The script to use
     * @param params Parameters that the script can use (can be <tt>null</tt>).
     */
    public ValleyRequestBuilder addScriptField(String name, String lang, String script, Map<String, Object> params) {
        sourceBuilder().scriptField(name, lang, script, params);
        return this;
    }

    /**
     * Adds a sort against the given field name and the sort ordering.
     *
     * @param field The name of the field
     * @param order The sort ordering
     */
    public ValleyRequestBuilder addSort(String field, SortOrder order) {
        sourceBuilder().sort(field, order);
        return this;
    }

    /**
     * Adds a generic sort builder.
     *
     * @see org.elasticsearch.search.sort.SortBuilders
     */
    public ValleyRequestBuilder addSort(SortBuilder sort) {
        sourceBuilder().sort(sort);
        return this;
    }

    /**
     * Applies when sorting, and controls if scores will be tracked as well. Defaults to
     * <tt>false</tt>.
     */
    public ValleyRequestBuilder setTrackScores(boolean trackScores) {
        sourceBuilder().trackScores(trackScores);
        return this;
    }

    /**
     * Adds the fields to load and return as part of the search request. If none are specified,
     * the source of the document will be returned.
     */
    public ValleyRequestBuilder addFields(String... fields) {
        sourceBuilder().fields(fields);
        return this;
    }

    /**
     * Adds a facet to the search operation.
     */
    public ValleyRequestBuilder addFacet(FacetBuilder facet) {
        sourceBuilder().facet(facet);
        return this;
    }

    /**
     * Sets a raw (xcontent) binary representation of facets to use.
     */
    public ValleyRequestBuilder setFacets(byte[] facets) {
        sourceBuilder().facets(facets);
        return this;
    }

    /**
     * Sets a raw (xcontent) binary representation of facets to use.
     */
    public ValleyRequestBuilder setFacets(byte[] facets, int facetsOffset, int facetsLength) {
        sourceBuilder().facets(facets, facetsOffset, facetsLength);
        return this;
    }

    /**
     * Sets a raw (xcontent) binary representation of facets to use.
     */
    public ValleyRequestBuilder setFacets(XContentBuilder facets) {
        sourceBuilder().facets(facets);
        return this;
    }

    /**
     * Sets a raw (xcontent) binary representation of facets to use.
     */
    public ValleyRequestBuilder setFacets(Map facets) {
        sourceBuilder().facets(facets);
        return this;
    }

    /**
     * Adds a field to be highlighted with default fragment size of 100 characters, and
     * default number of fragments of 5.
     *
     * @param name The field to highlight
     */
    public ValleyRequestBuilder addHighlightedField(String name) {
        highlightBuilder().field(name);
        return this;
    }


    /**
     * Adds a field to be highlighted with a provided fragment size (in characters), and
     * default number of fragments of 5.
     *
     * @param name         The field to highlight
     * @param fragmentSize The size of a fragment in characters
     */
    public ValleyRequestBuilder addHighlightedField(String name, int fragmentSize) {
        highlightBuilder().field(name, fragmentSize);
        return this;
    }

    /**
     * Adds a field to be highlighted with a provided fragment size (in characters), and
     * a provided (maximum) number of fragments.
     *
     * @param name              The field to highlight
     * @param fragmentSize      The size of a fragment in characters
     * @param numberOfFragments The (maximum) number of fragments
     */
    public ValleyRequestBuilder addHighlightedField(String name, int fragmentSize, int numberOfFragments) {
        highlightBuilder().field(name, fragmentSize, numberOfFragments);
        return this;
    }

    /**
     * Adds a field to be highlighted with a provided fragment size (in characters),
     * a provided (maximum) number of fragments and an offset for the highlight.
     *
     * @param name              The field to highlight
     * @param fragmentSize      The size of a fragment in characters
     * @param numberOfFragments The (maximum) number of fragments
     */
    public ValleyRequestBuilder addHighlightedField(String name, int fragmentSize, int numberOfFragments,
                                                    int fragmentOffset) {
        highlightBuilder().field(name, fragmentSize, numberOfFragments, fragmentOffset);
        return this;
    }

    /**
     * Set a tag scheme that encapsulates a built in pre and post tags. The allows schemes
     * are <tt>styled</tt> and <tt>default</tt>.
     *
     * @param schemaName The tag scheme name
     */
    public ValleyRequestBuilder setHighlighterTagsSchema(String schemaName) {
        highlightBuilder().tagsSchema(schemaName);
        return this;
    }

    /**
     * Explicitly set the pre tags that will be used for highlighting.
     */
    public ValleyRequestBuilder setHighlighterPreTags(String... preTags) {
        highlightBuilder().preTags(preTags);
        return this;
    }

    /**
     * Explicitly set the post tags that will be used for highlighting.
     */
    public ValleyRequestBuilder setHighlighterPostTags(String... postTags) {
        highlightBuilder().postTags(postTags);
        return this;
    }

    /**
     * The order of fragments per field. By default, ordered by the order in the
     * highlighted text. Can be <tt>score</tt>, which then it will be ordered
     * by score of the fragments.
     */
    public ValleyRequestBuilder setHighlighterOrder(String order) {
        highlightBuilder().order(order);
        return this;
    }


    /**
     * The encoder to set for highlighting
     */
    public ValleyRequestBuilder setHighlighterEncoder(String encoder) {
        highlightBuilder().encoder(encoder);
        return this;
    }

    /**
     * Sets the source of the request as a json string. Note, settings anything other
     * than the search type will cause this source to be overridden, consider using
     * {@link #setExtraSource(String)}.
     */
    public ValleyRequestBuilder setSource(String source) {
        request.source(source);
        return this;
    }

    /**
     * Sets the source of the request as a json string. Allows to set other parameters.
     */
    public ValleyRequestBuilder setExtraSource(String source) {
        request.extraSource(source);
        return this;
    }

    /**
     * Sets the source of the request as a json string. Note, settings anything other
     * than the search type will cause this source to be overridden, consider using
     * {@link #setExtraSource(byte[])}.
     */
    public ValleyRequestBuilder setSource(byte[] source) {
        request.source(source);
        return this;
    }

    /**
     * Sets the source of the request as a json string. Allows to set other parameters.
     */
    public ValleyRequestBuilder setExtraSource(byte[] source) {
        request.extraSource(source);
        return this;
    }

    /**
     * Sets the source of the request as a json string. Note, settings anything other
     * than the search type will cause this source to be overridden, consider using
     * {@link #setExtraSource(byte[])}.
     */
    public ValleyRequestBuilder setSource(byte[] source, int offset, int length) {
        request.source(source, offset, length);
        return this;
    }

    /**
     * Sets the source of the request as a json string. Allows to set other parameters.
     */
    public ValleyRequestBuilder setExtraSource(byte[] source, int offset, int length) {
        request.extraSource(source, offset, length);
        return this;
    }

    /**
     * Sets the source of the request as a json string. Note, settings anything other
     * than the search type will cause this source to be overridden, consider using
     * {@link #setExtraSource(byte[])}.
     */
    public ValleyRequestBuilder setSource(XContentBuilder builder) {
        request.source(builder);
        return this;
    }

    /**
     * Sets the source of the request as a json string. Allows to set other parameters.
     */
    public ValleyRequestBuilder setExtraSource(XContentBuilder builder) {
        request.extraSource(builder);
        return this;
    }

    /**
     * Sets the source of the request as a map. Note, setting anything other than the
     * search type will cause this source to be overridden, consider using
     * {@link #setExtraSource(java.util.Map)}.
     */
    public ValleyRequestBuilder setSource(Map source) {
        request.source(source);
        return this;
    }

    public ValleyRequestBuilder setExtraSource(Map source) {
        request.extraSource(source);
        return this;
    }

    
    
        /**
     * Sets the source builder to be used with this request. Note, any operations done
     * on this require builder before are discarded as this internal builder replaces
     * what has been built up until this point.
     */
    public ValleyRequestBuilder internalBuilder(ValleySourceBuilder sourceBuilder) {
        this.sourceBuilder = sourceBuilder;
        return this;
    }

    /**
     * Returns the internal search source builder used to construct the request.
     */
    public SearchSourceBuilder internalBuilder() {
        return sourceBuilder();
    }

    @Override
    public String toString() {
        return internalBuilder().toString();
    }

    @Override
    public ValleyRequest request() {
        if (sourceBuilder != null) {
            request.source(sourceBuilder());
        }
        return request;
    }

   

    protected void doInnerExecute(ActionListener<SearchResponse> listener) {
        if (sourceBuilder != null) {
            request.source( sourceBuilder());
        }
        ((Client) client).search(request, listener);
    }

    private ValleySourceBuilder sourceBuilder() {
        if (sourceBuilder == null) {
            sourceBuilder = new ValleySourceBuilder();
        }
        return sourceBuilder;
    }
    private HighlightBuilder highlightBuilder() {
        return sourceBuilder().highlighter();
    }

}
