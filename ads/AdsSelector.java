package io.bittiger.ads;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import net.spy.memcached.MemcachedClient;

public class AdsSelector {
	private static AdsSelector instance = null;
	//private int EXP = 7200;
	private String mMemcachedServer;
	private int mMemcachedPortal;
	private int mFeatureMemcachedPortal;
	String m_logistic_reg_model_file;
	String m_gbdt_model_path;
	private String mysql_host;
	private String mysql_db;
	private String mysql_user;
	private String mysql_pass;
	protected AdsSelector(String memcachedServer,int memcachedPortal,int featureMemcachedPortal,String logistic_reg_model_file, String gbdt_model_path,String mysqlHost,String mysqlDb,String user,String pass)
	{
		mMemcachedServer = memcachedServer;
		mMemcachedPortal = memcachedPortal;	
		mFeatureMemcachedPortal = featureMemcachedPortal;
		mysql_host = mysqlHost;
		mysql_db = mysqlDb;	
		mysql_user = user;
		mysql_pass = pass;
		m_logistic_reg_model_file = logistic_reg_model_file;
		m_gbdt_model_path = gbdt_model_path;
	}
	public static AdsSelector getInstance(String memcachedServer,int memcachedPortal,int featureMemcachedPortal, String logistic_reg_model_file, String gbdt_model_path,String mysqlHost,String mysqlDb,String user,String pass) {
	      if(instance == null) {
	         instance = new AdsSelector(memcachedServer, memcachedPortal,featureMemcachedPortal, logistic_reg_model_file,gbdt_model_path, mysqlHost,mysqlDb,user,pass);
	      }
	      return instance;
    }
	public List<Ad> selectAds(List<String> queryTerms,String device_id, String device_ip, String query_category)
	{
		List<Ad> adList = new ArrayList<Ad>();
		HashMap<Long,Integer> matchedAds = new HashMap<Long,Integer>();
		try {
			MemcachedClient cache = new MemcachedClient(new InetSocketAddress(mMemcachedServer,mMemcachedPortal));

			for(String queryTerm : queryTerms)
			{
				System.out.println("selectAds queryTerm = " + queryTerm);
				@SuppressWarnings("unchecked")
				Set<Long>  adIdList = (Set<Long>)cache.get(queryTerm);
				if(adIdList != null && adIdList.size() > 0)
				{
					for(Object adId : adIdList)
					{
						Long key = (Long)adId;
						if(matchedAds.containsKey(key))
						{
							int count = matchedAds.get(key) + 1;
							matchedAds.put(key, count);
						}
						else
						{
							matchedAds.put(key, 1);
						}
					}
				}				
			}
			
			for(Long adId : matchedAds.keySet())
			{			
				System.out.println("selectAds adId = " + adId);
				MySQLAccess mysql = new MySQLAccess(mysql_host, mysql_db, mysql_user, mysql_pass);
				Ad  ad = mysql.getAdData(adId);
				double relevanceScore = (double) (matchedAds.get(adId) * 1.0 / ad.keyWords.size());
				ad.relevanceScore = relevanceScore;
				System.out.println("selectAds initial relevanceScore = " + ad.relevanceScore);
				adList.add(ad);
			}
			//calculate pClick
			MemcachedClient featureCache = new MemcachedClient(new InetSocketAddress(mMemcachedServer,mFeatureMemcachedPortal));
						
			//net.spy.memcached.HashAlgorithmRegistry.registerHashAlgorithm()
			//net.spy.memcached.HashAlgorithmRegistry.registerHashAlgorithm(name, alg);
			System.out.println("mFeatureMemcachedPortal = " + mFeatureMemcachedPortal);

			for(Ad ad : adList) {
				//construct features, note that the order of features must be as follows
				ArrayList<Double> features = new ArrayList<Double>();

				//device_ip_click
				String device_ip_click_key = "dipc_" + device_ip;
				@SuppressWarnings("unchecked")
				String device_ip_click_val_str = (String)featureCache.get(device_ip_click_key);
			    Double device_ip_click_val = 0.0;
				if (device_ip_click_val_str != null && device_ip_click_val_str!= "") {
					device_ip_click_val = Double.parseDouble(device_ip_click_val_str);
				}
				features.add(device_ip_click_val);

				System.out.println("device_ip_click_key = " + device_ip_click_key);		
				System.out.println("device_ip_click_val = " + device_ip_click_val);		

				//device_ip_impression
				String device_ip_impression_key = "dipi_" + device_ip;
				@SuppressWarnings("unchecked")
				String device_ip_impression_val_str = (String)featureCache.get(device_ip_impression_key);
				Double device_ip_impression_val = 0.0;
				if (device_ip_impression_val_str != null && device_ip_impression_val_str!= "") {
					device_ip_impression_val = Double.parseDouble(device_ip_impression_val_str);
				}
				features.add(device_ip_impression_val);
				//System.out.println("device_ip_impression_key = " + device_ip_impression_key);		
				System.out.println("device_ip_impression_val = " + device_ip_impression_val);	

				
				//device_id_click
				String device_id_click_key = "didc_" + device_id;
				@SuppressWarnings("unchecked")
				String device_id_click_val_str = (String)featureCache.get(device_id_click_key);
				Double device_id_click_val = 0.0;
				if (device_id_click_val_str != null && device_id_click_val_str!= "") {
					device_id_click_val = Double.parseDouble(device_id_click_val_str);
				}
				features.add(device_id_click_val);
				System.out.println("device_id_click_key = " + device_id_click_key);		
				System.out.println("device_id_click_val = " + device_id_click_val);	
				System.out.println("device_id_click_val_str = " + device_id_click_val_str);	
				
				//device_id_impression
				String device_id_impression_key = "didi_" + device_id;
				@SuppressWarnings("unchecked")
				String device_id_impression_val_str = (String)featureCache.get(device_id_impression_key);
				Double device_id_impression_val = 0.0;
				if (device_id_impression_val_str != null && device_id_impression_val_str!= "") {
					device_id_impression_val = Double.parseDouble(device_id_impression_val_str);
				}
				features.add(device_id_impression_val);
				System.out.println("device_id_impression_key = " + device_id_impression_key);		
				System.out.println("device_id_impression_val = " + device_id_impression_val);	
				
				//ad_id_click
				String ad_id_click_key = "aidc_" + ad.adId.toString();
				@SuppressWarnings("unchecked")
				String ad_id_click_val_str = (String)featureCache.get(ad_id_click_key);
				Double ad_id_click_val = 0.0;
				if (ad_id_click_val_str != null && ad_id_click_val_str!= "") {
					ad_id_click_val = Double.parseDouble(ad_id_click_val_str);
				}
				features.add(ad_id_click_val);

				//ad_id_impression
				String ad_id_impression_key = "aidi_" + ad.adId.toString();
				@SuppressWarnings("unchecked")
				String ad_id_impression_val_str = (String)featureCache.get(ad_id_impression_key);
				Double ad_id_impression_val = 0.0;
				if (ad_id_impression_val_str != null && ad_id_impression_val_str!= "") {
					ad_id_impression_val = Double.parseDouble(ad_id_impression_val_str);
				}
				features.add(ad_id_impression_val);

				String query = Utility.strJoin(queryTerms, "_");
				//query_campaign_id_click
				String query_campaign_id_click_key = "qcidc_" + query + "_" + ad.campaignId.toString();
				@SuppressWarnings("unchecked")
				String query_campaign_id_click_val_str = (String)featureCache.get(query_campaign_id_click_key);
				Double query_campaign_id_click_val = 0.0;
				if (query_campaign_id_click_val_str != null && query_campaign_id_click_val_str!= "") {
					query_campaign_id_click_val = Double.parseDouble(query_campaign_id_click_val_str);
				}
				features.add(query_campaign_id_click_val);
				
				//query_campaign_id_impression
				String query_campaign_id_impression_key = "qcidi_" + query + "_" + ad.campaignId.toString();
				@SuppressWarnings("unchecked")
				String query_campaign_id_impression_val_str = (String)featureCache.get(query_campaign_id_impression_key);
				Double query_campaign_id_impression_val = 0.0;
				if (query_campaign_id_impression_val_str != null && query_campaign_id_impression_val_str!= "") {
					query_campaign_id_impression_val = Double.parseDouble(query_campaign_id_impression_val_str);
				}
				features.add(query_campaign_id_impression_val);
				
				//query_ad_id_click
				String query_ad_id_click_key = "qaidc_" + query + "_" + ad.adId.toString();
				@SuppressWarnings("unchecked")
				String query_ad_id_click_val_str = (String)featureCache.get(query_ad_id_click_key);
				Double query_ad_id_click_val = 0.0;
				if (query_ad_id_click_val_str != null && query_ad_id_click_val_str!= "") {
					query_ad_id_click_val = Double.parseDouble(query_ad_id_click_val_str);
				}
				features.add(query_ad_id_click_val);

				//query_ad_id_impression
				String query_ad_id_impression_key = "qaidi_" + query + "_" + ad.adId.toString();
				@SuppressWarnings("unchecked")
				String query_ad_id_impression_val_str = (String)featureCache.get(query_ad_id_impression_key);
				Double query_ad_id_impression_val = 0.0;
				if (query_ad_id_impression_val_str != null && query_ad_id_impression_val_str!= "") {
					query_ad_id_impression_val = Double.parseDouble(query_ad_id_impression_val_str);
				}
				features.add(query_ad_id_impression_val);

				//query_ad_category_match scale to 1000000 if match
				double query_ad_category_match = 0.0;
				if(query_category == ad.category) {
					query_ad_category_match = 1000000.0;
				}
				features.add(query_ad_category_match);
				
				ad.pClick = CTRModel.getInstance(m_logistic_reg_model_file, m_gbdt_model_path).predictCTRWithLogisticRegression(features);
				System.out.println("ad.pClick = " + ad.pClick);
			}		
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return adList;
	}
}
