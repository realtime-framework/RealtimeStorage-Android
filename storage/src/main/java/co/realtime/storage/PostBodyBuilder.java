package co.realtime.storage;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;

class PostBodyBuilder {
	StorageContext context;
	Map <String, Object> body;
	
	PostBodyBuilder(StorageContext context){
		this.context = context;
		body = new HashMap<String, Object>();
		body.put("applicationKey", context.applicationKey);
		// TODO: remove this check after removing the private key
		if(context.privateKey!=null)
			body.put("privateKey", context.privateKey);
		if(context.authenticationToken != null)
			body.put("authenticationToken", context.authenticationToken);
	}
	
	void addObject(String key, Object value){
		body.put(key, value);
	}
	
	Object getObject(String key){
		return body.get(key);
	}
	
	String getBody() throws JsonProcessingException{		
		return context.mapper.writeValueAsString(this.body);
	}
}
