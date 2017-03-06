package io.bittiger.ads;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import net.spy.memcached.MemcachedClient;


public class QueryParser {
	private static QueryParser instance = null;
	
	protected QueryParser() {
		
	}
	public static QueryParser getInstance() {
	      if(instance == null) {
	         instance = new QueryParser();
	      }
	      return instance;
    }
	public List<String> QueryUnderstand(String query) {
		List<String> tokens = Utility.cleanedTokenize(query);
		return tokens;
	}
	
	public List<List<String>> QueryRewrite(String query, String memcachedServer,int memcachedPortal) {
		List<List<String>> res = new ArrayList<List<String>>();
		List<String> tokens = Utility.cleanedTokenize(query);
		String query_key = Utility.strJoin(tokens, "_");
		try {
			MemcachedClient cache = new MemcachedClient(new InetSocketAddress(memcachedServer, memcachedPortal));
			if(cache.get(query_key) instanceof List) {
				@SuppressWarnings("unchecked")
				List<String>  synonyms = (ArrayList<String>)cache.get(query_key);
				for(String synonym : synonyms) {
					List<String> token_list = new ArrayList<String>();
					String[] s = synonym.split("_");
					for(String w : s) {
						token_list.add(w);
					}
					res.add(token_list);
				}			
			}
			else {
				res.add(tokens);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	 
		return res;
	}
}
