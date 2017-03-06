package io.bittiger.ads;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import java.util.ArrayList;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import org.json.*;

public class AdsEngine {
	private String mAdsDataFilePath;
	private String mBudgetFilePath;
	String m_logistic_reg_model_file;
	String m_gbdt_model_path;
	private IndexBuilder indexBuilder;
	private String mMemcachedServer;
	private int mMemcachedPortal;
	private int mFeatureMemcachedPortal;
	private int mSynonymsMemcachedPortal;
	private String mysql_host;
	private String mysql_db;
	private String mysql_user;
	private String mysql_pass;
	
	public AdsEngine(String adsDataFilePath, String budgetDataFilePath,String logistic_reg_model_file, 
			String gbdt_model_path, String memcachedServer,int memcachedPortal,int featureMemcachedPortal,int synonymsMemcachedPortal,
			String mysqlHost,String mysqlDb,String user,String pass)
	{
		mAdsDataFilePath = adsDataFilePath;
		mBudgetFilePath = budgetDataFilePath;
		m_logistic_reg_model_file = logistic_reg_model_file;
		m_gbdt_model_path = gbdt_model_path;
		mMemcachedServer = memcachedServer;
		mMemcachedPortal = memcachedPortal;
		mFeatureMemcachedPortal = featureMemcachedPortal;
		mSynonymsMemcachedPortal = synonymsMemcachedPortal;
		mysql_host = mysqlHost;
		mysql_db = mysqlDb;	
		mysql_user = user;
		mysql_pass = pass;	
		indexBuilder = new IndexBuilder(memcachedServer,memcachedPortal,mysql_host,mysql_db,mysql_user,mysql_pass);
	}
	
	public Boolean init()
	{
		//load ads data
		try (BufferedReader brAd = new BufferedReader(new FileReader(mAdsDataFilePath))) {
			String line;
			while ((line = brAd.readLine()) != null) {
				JSONObject adJson = new JSONObject(line);
				Ad ad = new Ad(); 
				if(adJson.isNull("adId") || adJson.isNull("campaignId")) {
					continue;
				}
				ad.adId = adJson.getLong("adId");
				ad.campaignId = adJson.getLong("campaignId");
				ad.brand = adJson.isNull("brand") ? "" : adJson.getString("brand");
				ad.price = adJson.isNull("price") ? 100.0 : adJson.getDouble("price");
				ad.thumbnail = adJson.isNull("thumbnail") ? "" : adJson.getString("thumbnail");
				ad.title = adJson.isNull("title") ? "" : adJson.getString("title");
				ad.detail_url = adJson.isNull("detail_url") ? "" : adJson.getString("detail_url");						
				ad.bidPrice = adJson.isNull("bidPrice") ? 1.0 : adJson.getDouble("bidPrice");
				ad.pClick = adJson.isNull("pClick") ? 0.0 : adJson.getDouble("pClick");
				ad.category =  adJson.isNull("category") ? "" : adJson.getString("category");
				ad.description = adJson.isNull("description") ? "" : adJson.getString("description");
				ad.keyWords = new ArrayList<String>();
				JSONArray keyWords = adJson.isNull("keyWords") ? null :  adJson.getJSONArray("keyWords");
				for(int j = 0; j < keyWords.length();j++)
				{
					ad.keyWords.add(keyWords.getString(j));
				}
				
				if(!indexBuilder.buildInvertIndex(ad) || !indexBuilder.buildForwardIndex(ad))
				{
					//log				
				}
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
		
		//load budget data
		try (BufferedReader brBudget = new BufferedReader(new FileReader(mBudgetFilePath))) {
			String line;
			while ((line = brBudget.readLine()) != null) {
				JSONObject campaignJson = new JSONObject(line);
				Long campaignId = campaignJson.getLong("campaignId");
				double budget = campaignJson.getDouble("budget");
				Campaign camp = new Campaign();
				camp.campaignId = campaignId;
				camp.budget = budget;
				if(!indexBuilder.updateBudget(camp))
				{
					//log
				}			
			}
		}catch (IOException e) {
			e.printStackTrace();
		}
		return true;
	}
	public List<Ad> selectAds(String query, String device_id, String device_ip, String query_category)
	{
		//query understanding
		//List<String> queryTerms = QueryParser.getInstance().QueryUnderstand(query);
		List<List<String>> rewrittenQuery =  QueryParser.getInstance().QueryRewrite(query, mMemcachedServer, mSynonymsMemcachedPortal);
		List<Ad> adsCandidates = new ArrayList<Ad>();
		Set<Long> uniquueAds = new HashSet<Long>();
		
		//select ads candidates
		for (List<String> queryTerms : rewrittenQuery) {	
			List<Ad> adsCandidates_temp = AdsSelector.getInstance(mMemcachedServer, mMemcachedPortal,mFeatureMemcachedPortal,
					m_logistic_reg_model_file,m_gbdt_model_path, mysql_host, mysql_db,mysql_user, mysql_pass).selectAds(queryTerms,device_id, device_ip, query_category);	
			for(Ad ad : adsCandidates_temp) {
				if (!uniquueAds.contains(ad.adId)) {
					adsCandidates.add(ad);
				}
			}
		}
		//TODO: give rewritten query lower rank score
		
		//L0 filter by pClick, relevance score
		List<Ad> L0unfilteredAds = AdsFilter.getInstance().LevelZeroFilterAds(adsCandidates);
		System.out.println("L0unfilteredAds ads left = " + L0unfilteredAds.size());

		//rank 
		List<Ad> rankedAds = AdsRanker.getInstance().rankAds(L0unfilteredAds);
		System.out.println("rankedAds ads left = " + rankedAds.size());

		//L1 filter by relevance score : select top K ads
		int k = 20;
		List<Ad> unfilteredAds = AdsFilter.getInstance().LevelOneFilterAds(rankedAds,k);
		System.out.println("unfilteredAds ads left = " + unfilteredAds.size());

		//Dedupe ads per campaign
		List<Ad> dedupedAds = AdsCampaignManager.getInstance(mysql_host, mysql_db,mysql_user, mysql_pass).DedupeByCampaignId(unfilteredAds);
		System.out.println("dedupedAds ads left = " + dedupedAds.size());

		//pricing： next rank score/current score * current bid price
		AdPricing.getInstance().setCostPerClick(dedupedAds);
		//filter last one , ad without budget , ads with CPC < minReservePrice
		List<Ad> ads = AdsCampaignManager.getInstance(mysql_host, mysql_db,mysql_user, mysql_pass).ApplyBudget(dedupedAds);
		System.out.println("AdsCampaignManager ads left = " + ads.size());

		//allocation
		AdsAllocation.getInstance().AllocateAds(ads);
		return ads;
	}
}
