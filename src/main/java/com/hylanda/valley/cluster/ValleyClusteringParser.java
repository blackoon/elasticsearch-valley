package com.hylanda.valley.cluster;

import java.io.IOException;
import java.util.Collections;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContentParser;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.NonCollectingAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSource.Bytes;
import org.elasticsearch.search.aggregations.support.ValuesSource.Numeric;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceParser;
import org.elasticsearch.search.fetch.FetchPhase;
import org.elasticsearch.search.fetch.fielddata.FieldDataFieldsParseElement;
import org.elasticsearch.search.fetch.script.ScriptFieldsParseElement;
import org.elasticsearch.search.fetch.source.FetchSourceParseElement;
import org.elasticsearch.search.highlight.HighlighterParseElement;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.SubSearchContext;
import org.elasticsearch.search.sort.SortParseElement;

import com.hylanda.valley.output.InternalValleyClustering;
import com.hylanda.valley.output.InternalValleyClustering.Bucket;

/**
 * @author zhangy
 * @E-mail:blackoon88@gmail.com
 * @version 创建时间：2016年7月19日 上午10:16:20 note
 */
public class ValleyClusteringParser implements Aggregator.Parser {

	private final FetchPhase fetchPhase;

	@Inject
	public ValleyClusteringParser(FetchPhase fetchPhase,
			SortParseElement sortParseElement,
			FetchSourceParseElement sourceParseElement,
			HighlighterParseElement highlighterParseElement,
			FieldDataFieldsParseElement fieldDataFieldsParseElement,
			ScriptFieldsParseElement scriptFieldsParseElement) {
		this.fetchPhase = fetchPhase;
	}
	
	@Override
	public String type() {
		return InternalValleyClustering.TYPENAME;
	}
	
	@Override
	public AggregatorFactory parse(String aggregationName,
			XContentParser parser, SearchContext context) throws IOException {
		SubSearchContext subSearchContext = new SubSearchContext(context);
		ValuesSourceParser<Numeric> vsParser = ValuesSourceParser.numeric(aggregationName, InternalValleyClustering.TYPE,context).scriptable(false).build();
		XContentParser.Token token;
		String currentFieldName = null;
		int diff = 3;
		String field = "simhash";
		String idField = "_id";
		String fields = null;
		String method = "single-pass";
		while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
			if (token == XContentParser.Token.VALUE_STRING) {
				if ("field".equals(currentFieldName)) {
					field = parser.text();
				} else if ("idfield".equals(currentFieldName)) {
					idField = parser.text();
				} else if ("fields".equals(currentFieldName)) {
					fields = parser.text();
				} else if ("method".equals(currentFieldName)) {
					method = parser.text();
				}
			}
			if (token == XContentParser.Token.FIELD_NAME) {
				currentFieldName = parser.currentName();
			} else if (vsParser.token(currentFieldName, token, parser)) {
				continue;
			} else if (token == XContentParser.Token.VALUE_NUMBER) {
				if ("diff".equals(currentFieldName)) {
					diff = parser.intValue();
				}
			}
		}
		ValuesSourceParser<Bytes> vsParserId = ValuesSourceParser
				.bytes(aggregationName, InternalValleyClustering.TYPE, context)
				.scriptable(false).build();

		final String buketIdField = idField;
		vsParserId.token("field", XContentParser.Token.VALUE_STRING,
				new JsonXContentParser(null) {
					@Override
					public String text() throws IOException {
						return buketIdField;
					}
				});

		ChildFields childFields = null;
		if (fields != null) {
			String[] fieldsList = fields.split(",");
			childFields = new ChildFields();
			for (String f : fieldsList) {
				ValuesSourceParser<Bytes> item = ValuesSourceParser
						.bytes(aggregationName, InternalValleyClustering.TYPE,
								context).scriptable(false).build();

				final String buketIdField2 = f;
				item.token("field", XContentParser.Token.VALUE_STRING,
						new JsonXContentParser(null) {
							@Override
							public String text() throws IOException {
								return buketIdField2;
							}
						});
				childFields.addFiled(f, item.config());
			}
		}
		return new ValleyFactory(aggregationName, method, fetchPhase,
				subSearchContext, vsParser.config(), vsParserId.config(),
				field, diff, childFields);
	}
	private static class ValleyFactory extends ValuesSourceAggregatorFactory<ValuesSource.Numeric> {

		private final FetchPhase fetchPhase;
        private final SubSearchContext subSearchContext;
//		private final String field;
		private final int diff;
		private final String method;
		private ValuesSourceConfig<Bytes> configId;
		private ChildFields childFields;
		public ValleyFactory(String name, String method, FetchPhase fetchPhase, SubSearchContext subSearchContext, ValuesSourceConfig<Numeric> config, ValuesSourceConfig<Bytes> configId, String field, int diff, ChildFields childFields) {
			super(name, InternalValleyClustering.TYPE.name(), config);
			this.fetchPhase = fetchPhase;
			this.subSearchContext = subSearchContext;
//			this.field = field;
			this.diff = diff;
			this.method = method;
			this.configId = configId;
			this.childFields = childFields;
		}

		@Override
		protected Aggregator createUnmapped(
				AggregationContext aggregationContext, Aggregator parent) {
			final InternalAggregation aggregation = new InternalValleyClustering(name,  diff,  Collections.<Bucket>emptyList());
			return new NonCollectingAggregator(name, aggregationContext, parent) {
                @Override
                public InternalAggregation buildEmptyAggregation() {
                    return aggregation;
                }
            };
		}

		@Override
		protected Aggregator create(Numeric valuesSource,
				long expectedBucketsCount,
				AggregationContext aggregationContext, Aggregator parent) {
			ValuesSource.Bytes valuesSourceId = aggregationContext.valuesSource(configId, parent == null ? 0 : 1 + parent.depth());
//			ValuesSource.Bytes[] valuesSourceFields= null;
			if(childFields != null) {
				childFields.apply(aggregationContext,  parent == null ? 0 : 1 + parent.depth());
			}
			return new ValleyAggregator(fetchPhase, subSearchContext, name, method, factories, valuesSource, valuesSourceId,  diff , aggregationContext, parent, childFields);
		}
	
	}
}
