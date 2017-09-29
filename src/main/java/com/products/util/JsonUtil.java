package com.products.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class JsonUtil {
	
	public static String objectToJson(Object o){
		Gson gson = new GsonBuilder() .disableHtmlEscaping().create();
	    return gson.toJson(o) ;
	} 
 
	public static <T> T stringToObject(String json,Class<T> cls){
		Gson gson = new Gson();
		return gson.fromJson(json,cls);
	}

}
