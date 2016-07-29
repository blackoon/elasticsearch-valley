package com.hylanda.valley.cluster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.fetch.FetchPhase;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.SubSearchContext;

import com.hylanda.valley.output.InternalValleyClustering;
import com.hylanda.valley.output.InternalValleyClustering.Bucket;
import com.hylanda.valley.output.InternalValleyClustering.BucketChild;

/**
 * 基于simhash 值的聚类算法
 *
 */
public class ValleyAggregator extends Aggregator {

	private static final int INITIAL_CAPACITY = 50;
	private final ValuesSource.Numeric valuesSource;
	private final ValuesSource.Bytes valuesSourceId;
	private SortedNumericDocValues values;
	private SortedBinaryDocValues ids;
	private ChildFields childFields;
	private int diff;
	private final FetchPhase fetchPhase;
	private final SubSearchContext subSearchContext;
	private AtomicReaderContext currentContext;
	private List<Bucket> buckets = new ArrayList<Bucket>();
	private int docCount = 0;

	public ValleyAggregator(FetchPhase fetchPhase,
			SubSearchContext subSearchContext, String name, String method,
			AggregatorFactories factories, ValuesSource.Numeric valuesSource,
			ValuesSource.Bytes valuesSourceId, int diff,
			AggregationContext aggregationContext, Aggregator parent,
			ChildFields childFields) {
		super(name, BucketAggregationMode.PER_BUCKET, factories,
				INITIAL_CAPACITY, aggregationContext, parent);
		this.fetchPhase = fetchPhase;
		this.subSearchContext = subSearchContext;
		this.valuesSource = valuesSource;
		this.valuesSourceId = valuesSourceId;
		this.childFields = childFields;
		this.diff = diff;
	}

	@Override
	public void setNextReader(AtomicReaderContext reader) {
		currentContext = reader;
		values = valuesSource.longValues();
		ids = valuesSourceId.bytesValues();
		if (childFields != null) {
			childFields.setNextReader(reader);
		}
	}

	@Override
	public boolean shouldCollect() {
		return true;
	}

	@Override
	public InternalAggregation buildAggregation(long owningBucketOrdinal) {
		assert owningBucketOrdinal == 0;
		if (buckets == null || buckets.size() == 0)
			return buildEmptyAggregation();

		// 我们再次对只有一个分类的做一下优化处理
		compact();

		int fetchBucketCount = buckets.size();
		if (fetchBucketCount > 1000)
			fetchBucketCount = 1000;

		int[] docIdsToLoad = new int[fetchBucketCount];
		TopDocs tp = new TopDocs(docIdsToLoad.length, new ScoreDoc[0], 1);
		subSearchContext.queryResult().topDocs(tp);

		for (int i = 0; i < docIdsToLoad.length; i++) {
			docIdsToLoad[i] = buckets.get(i).getInternalId();
		}

		subSearchContext.docIdsToLoad(docIdsToLoad, 0, docIdsToLoad.length);
		fetchPhase.execute(subSearchContext);
		FetchSearchResult fetchResult = subSearchContext.fetchResult();
		InternalSearchHit[] internalHits = fetchResult.fetchResult().hits()
				.internalHits();
		for (int i = 0; i < internalHits.length; i++) {
			InternalSearchHit hit = internalHits[i];
			assert (hit.docId() == docIdsToLoad[i]);
			buckets.get(i).setSource(hit.internalSourceRef());
		}
		if (fetchBucketCount < buckets.size()) {
			buckets = buckets.subList(0, fetchBucketCount);
		}
		return new InternalValleyClustering(name, diff, buckets);
	}

	@Override
	public InternalAggregation buildEmptyAggregation() {
		return new InternalValleyClustering(name, diff,
				Collections.<Bucket> emptyList());
	}

	@Override
	public void collect(int docId, long bucketOrdinal) throws IOException {
		assert bucketOrdinal == 0;
		if (docCount > 10000)
			return;

		values.setDocument(docId);
		ids.setDocument(docId);
		if (childFields != null) {
			childFields.setDocument(docId);
		}
		int valuesCount = values.count();
		int idCount = ids.count();
		for (int i = 0; i < valuesCount; ++i) {
			long v = values.valueAt(i);
			String id = idCount > 0 ? ids.valueAt(0).utf8ToString() : "";
			BucketChild child = new BucketChild();
			child.setDoc_id(id);

			if (childFields != null) {
				for (ChildFields.Field f : childFields.getFields().values()) {
					int count = f.fieldValues.count();
					if (count > 0) {
						child.put(f.name, f.fieldValues.valueAt(0)
								.utf8ToString());
					}
				}
			}
			insertBucket(v, docId + currentContext.docBase, id, child);
		}

		docCount++;

	}

	protected void compact() {
		if (buckets.size() == 1)
			return;

		// 现在把顶层数据合并一下？
		// 不做了
		Collections.sort(buckets, new Comparator<Bucket>() {

			@Override
			public int compare(Bucket o1, Bucket o2) {
				return Long.compare(o2.getDocCount(), o1.getDocCount());
			}

		});
	}

	protected void insertBucket(long hash, int internalId, String docId,
			BucketChild child) {

		// for(Bucket item : buckets) {
		// int d = SimHash.hammingDistance(hash, item.getKeyAsLong());
		// if(d <= diff) {
		// //byte[] bytes=id.getBytes();
		// //BytesReference child =new BytesArray(bytes, 0, bytes.length);
		// item.addDoc(child);
		// return;
		// }
		// }

		Bucket item = new Bucket();
		item.setKeyValue(hash);
		item.setDocId(docId);

		// byte[] bytes=id.getBytes();
		// BytesReference child =new BytesReference();
		// BytesReference child =new BytesArray(bytes, 0, bytes.length);
		// item.addDoc(id);
		item.addDoc(child);
		item.setInternalId(internalId);
		buckets.add(item);

	}

}
