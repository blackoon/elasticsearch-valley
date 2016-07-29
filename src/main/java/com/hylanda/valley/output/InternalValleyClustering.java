package com.hylanda.valley.output;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.text.StringText;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregation;

public class InternalValleyClustering extends InternalAggregation implements
		ValleyClustering {
	public static final Type TYPE = new Type(TYPENAME, "vyclustering");
	private int diff;
	
	private List<Bucket> buckets;
	private Map<Long, Bucket> bucketsMap;
	
	private InternalValleyClustering() {
		diff = 3;
	}

	public InternalValleyClustering(String name, int diff, List<Bucket> buckets) {
		super(name);
		this.diff = diff;
		this.buckets = buckets;
	}
	public static final AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
		@Override
		public InternalValleyClustering readResult(StreamInput in)
				throws IOException {
			InternalValleyClustering buckets = new InternalValleyClustering();
			buckets.readFrom(in);
			return buckets;
		}
	};

	public static class HASHITEM implements Comparable<HASHITEM> {
		public int diff;
		public long hash;

		public HASHITEM(int diff, long hash) {
			this.diff = diff;
			this.hash = hash;
		}

		@Override
		public int compareTo(HASHITEM o) {
			return Long.compare(hash, o.hash);
		}

	}
	
	@SuppressWarnings("serial")
	public static final class BucketChild extends HashMap<String, String> {
		
		public String getRelease_date() {
			return get("release_date");
		}

		public void setRelease_date(String release_date) {
			put("release_date", release_date);
		}

		public String getDoc_id() {
			return get("doc_id");
		}

		public void setDoc_id(String doc_id) {
			put("doc_id", doc_id);
		}
//		public String getUrl() {
//			return get("url");
//		}
//
//		public void setUrl(String url) {
//			put("url", url);
//		}
//
//		public String getTitle() {
//			return get("title");
//		}
//
//		public void setTitle(String title) {
//			put("title", title);
//		}
//
//		public String getMedia_name() {
//			return get("media_name");
//		}
//
//		public void setMedia_name(String media_name) {
//			put("media_name", media_name);
//		}
//
//		public String getContent_media_name() {
//			return get("content_media_name");
//		}
//
//		public void setContent_media_name(String content_media_name) {
//			put("content_media_name", content_media_name);
//		}
//
//		public String getAuthor() {
//			return get("author");
//		}
//
//		public void setAuthor(String author) {
//			put("author", author);
//		}
		
		
	}

	public static final class Bucket implements ClusteringBucket {

		private long key;
		private long docCount;
		private String docId;
		private Set<BucketChild> docs;
		private int internalId; // no write
		private BytesReference source;
		

		public Bucket() {
			docs = new HashSet<BucketChild>();
		}

		public Bucket(long key, long docCount, String docId, Set<BucketChild> docs) {
			this.key = key;
			this.docCount = docCount;
			this.docId = docId;
			this.docs = docs;
		}

		public String getKey() {
			return Long.toString(key, 16);
		}

		public Text getKeyAsText() {
			return new StringText(getKey());
		}

		public long getDocCount() {
			return docCount;
		}

		public Aggregations getAggregations() {
			return null;
		}

		public Set<BucketChild> getDocs() {
			return docs;
		}

		public Long getKeyAsLong() {
			return key;
		}

		public String getDocId() {
			return docId;
		}

		public int getInternalId() {
			return internalId;
		}

		public void setInternalId(int internalId) {
			this.internalId = internalId;
		}

		public void addDoc(BucketChild child) {
			docCount++;
			docs.add(child);
		}

		public void addDocs(long doCount, Set<BucketChild> children) {
			this.docCount += doCount;
			docs.addAll(children);
		}
		//应该是id 
		/*public boolean existDoc(BucketChild child) {
			return docs.contains(child);
		}*/

		public long getKeyValue() {
			return key;
		}

		public void setKeyValue(long v) {
			key = v;
		}

		public void setDocId(String docId) {
			this.docId = docId;
		}

		public void setSource(BytesReference source) {
			this.source = source;
		}

		public BytesReference getSource() {
			return this.source;
		}
		
	}

	

	public static void registerStreams() {
		AggregationStreams.registerStream(STREAM, TYPE.stream());
	}
	
	@Override
	public void readFrom(StreamInput in) throws IOException {
		this.name = in.readString();
		this.diff = in.readVInt();

		int size = in.readVInt();
		List<Bucket> buckets = new ArrayList<>(size);
		
		for (int i = 0; i < size; i++) {
			long key = in.readLong();
			long docCount = in.readVLong();
//			Set<BytesReference> sources =new HashSet<>();
			String docId = in.readString();
			BytesReference source = in.readBytesReference();
			int idCount = in.readVInt();
			Set<BucketChild> docs = new HashSet<BucketChild>();
			for (int k = 0; k < idCount; k++) {
				BucketChild e =new BucketChild();
				int count = in.readVInt();
				for(int p = 0; p < count; p++) {
					String name = in.readString();
					String value = in.readString();
					e.put(name,  value);
				}
				docs.add(e);
			}
			Bucket b = new Bucket(key, docCount, docId, docs);
			b.setSource(source);
			buckets.add(b);
		}
		this.buckets = buckets;
		this.bucketsMap = null;

	}

	@Override
	public void writeTo(StreamOutput out) throws IOException {
		out.writeString(name);
		out.writeVInt(diff);
		out.writeVInt(buckets.size());
		for (Bucket b : buckets) {
			out.writeLong(b.getKeyValue());
			out.writeVLong(b.getDocCount());
			out.writeString(b.docId);
			out.writeBytesReference(b.source);
			out.writeVInt(b.getDocs().size());
			for (BucketChild child : b.getDocs()) {
				out.writeVInt(child.size());
				for(Map.Entry<String, String> entry : child.entrySet()) {
					out.writeString(entry.getKey());
					out.writeString(entry.getValue());
				}
			}
		}

	}

	@Override
	public Type type() {
		return TYPE;
	}

	@Override
	public InternalAggregation reduce(ReduceContext reduceContext) {
		List<InternalAggregation> aggregations = reduceContext.aggregations();
		List<Bucket> buckets = null;

		long total = 0;
		for (InternalAggregation aggregation : aggregations) {
			InternalValleyClustering item = (InternalValleyClustering) aggregation;
			if (buckets == null) {
				buckets = new ArrayList<Bucket>();
				for (Bucket b : item.buckets) {
					buckets.add(b);
					total += b.getDocCount();
				}
				continue;
			}
			for (Bucket bucket : item.buckets) {
				total += bucket.getDocCount();

				boolean succ = false;
//				for (Bucket b : buckets) {
//					if (SimHash.hammingDistance(bucket.getKeyValue(),
//							b.getKeyValue()) <= diff) {
//						b.addDocs(bucket.getDocCount(), bucket.getDocs());
//						succ = true;
//						break;
//					}
//				}
				if (!succ) {
					buckets.add(bucket);
				}

			}
		}
		return new InternalValleyClustering(name, diff, buckets);
	}

	@Override
	public XContentBuilder doXContentBody(XContentBuilder builder, Params params)
			throws IOException {
		builder.startArray(CommonFields.BUCKETS);
		for (Bucket bucket : buckets) {
			builder.startObject();
			try {
				builder.field(CommonFields.KEY, bucket.getKey());
				builder.field(CommonFields.DOC_COUNT, bucket.getDocCount());
				builder.field("id", bucket.getDocId());
				BytesReference source = CompressorFactory.uncompressIfNeeded(bucket.getSource());
				XContentParser parser = XContentHelper.createParser(source);
				builder.field("source", parser.map());
				builder.startArray("ids");
				for (BucketChild id : bucket.getDocs()){
					builder.value(id);
				}
				builder.endArray();
			} catch (IOException e) {
				continue;
			}
			builder.endObject();
		}
		builder.endArray();
		return builder;
	}

	@SuppressWarnings("unchecked")
	@Override
	public ClusteringBucket getBucketByKey(String key) {
		if (bucketsMap == null) {
			bucketsMap = new HashMap<>(buckets.size());
			for (Bucket bucket : buckets) {
				bucketsMap.put(bucket.getKeyAsLong(), bucket);
			}
		}
		return bucketsMap.get(key);
	}

	@SuppressWarnings("unchecked")
	@Override
	public Collection<ClusteringBucket> getBuckets() {
		Object o = buckets;
		return (Collection<ClusteringBucket>) o;
	}

}
