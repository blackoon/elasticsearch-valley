package com.hylanda.valley.output;

import java.util.Collection;
import java.util.Set;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;

import com.hylanda.valley.output.InternalValleyClustering.BucketChild;


/**
 * 聚类对象
 * @author zhangy
 *
 */
public interface ValleyClustering extends MultiBucketsAggregation {
	public static final String TYPENAME = "valley_clustering";
	public static interface ClusteringBucket extends MultiBucketsAggregation.Bucket {
		BytesReference getSource();
	    Set<BucketChild> getDocs();
		Long getKeyAsLong();
		String getDocId();
	}
	@Override
    Collection<ClusteringBucket> getBuckets();
	
	@SuppressWarnings("unchecked")
	@Override
	ClusteringBucket getBucketByKey(String key);
}
