package com.hylanda.valley;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.GenericAction;
import org.elasticsearch.action.support.TransportAction;

import com.hylanda.valley.search.action.ValleyTransportAction;

/** 
 * @author zhangy
 * @E-mail:blackoon88@gmail.com 
 * @version 创建时间：2016年6月20日 下午4:22:39 
 * note
 */
public class App2 {
	public <Request extends ActionRequest, Response extends ActionResponse> void registerAction
	(GenericAction<Request, Response> action,
			Class<? extends TransportAction<Request,
			Response>> transportAction, Class... supportTransportActions) {
    }
	
	public static void main(String[] args) {
		List<Class> l=new ArrayList<Class>();
		l.add(ValleyTransportAction.class);
		//l.toArray(new Class[l.size()])
		new App2().registerAction(null, null, new Class[]{ValleyTransportAction.class} );
	}
}
