package com.hylanda.valley.search.action;

import java.io.IOException;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

/** 
 * @author zhangy
 * @E-mail:blackoon88@gmail.com 
 * @version 创建时间：2016年6月21日 下午4:54:55 
 * note
 */
public class ValleyBuilder implements ToXContent {
	 private String[] TitleFields;
	    private String[] SummaryFields;
	    private String Language;
	    private String Algorithm;
	    private int FetchSize=100;


	    public String[] getTitleFields() {
	        return TitleFields;
	    }

	    public void setTitleFields(String[] titleFields) {
	        TitleFields = titleFields;
	    }

	    public String[] getSummaryFields() {
	        return SummaryFields;
	    }

	    public void setSummaryFields(String[] summaryFields) {
	        SummaryFields = summaryFields;
	    }

	    public String getLanguage() {
	        return Language;
	    }

	    public void setLanguage(String language) {
	        Language = language;
	    }

	    public String getAlgorithm() {
	        return Algorithm;
	    }

	    public void setAlgorithm(String algorithm) {
	        Algorithm = algorithm;
	    }

	    public int getFetchSize() {
	        return FetchSize;
	    }

	    public void setFetchSize(int fetchSize) {
	        FetchSize = fetchSize;
	    }

	    public XContentBuilder toXContent(XContentBuilder xContentBuilder, Params params) throws IOException {
	        return null;
	    }
}
