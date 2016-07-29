package com.hylanda.valley.search.action;

import org.elasticsearch.search.builder.SearchSourceBuilder;

/** 
 * @author zhangy
 * @E-mail:blackoon88@gmail.com 
 * @version 创建时间：2016年6月20日 下午3:04:41 
 * note
 */
public class ValleySourceBuilder extends SearchSourceBuilder{

	private ValleyBuilder valleyBuilder;
	public ValleyBuilder valley(){
		if(valleyBuilder==null){
			valleyBuilder=new ValleyBuilder();
		}
		return valleyBuilder;
	}
	
	public ValleySourceBuilder valley(ValleyBuilder valleyBuilder){
		this.valleyBuilder=valleyBuilder;
		return this;
	}
}
