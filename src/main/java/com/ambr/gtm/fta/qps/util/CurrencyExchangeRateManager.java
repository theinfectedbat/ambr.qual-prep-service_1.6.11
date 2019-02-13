package com.ambr.gtm.fta.qps.util;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ambr.gtm.fta.qts.util.Env;
import com.ambr.platform.utils.log.MessageFormatter;

/**
 * This class is define as a Spring Bean.
 * It manage a Map having a County Mapping class as key and stored Rate of that day.
 * 
 * This get initialized by spring and can be use in application by AutoWearing
 * @author m2249
 *
 */
public class CurrencyExchangeRateManager 
{
	private Map<CountryMapping, Rate> m_ExchangeRateCache = new HashMap<>();
	 
	
	static Logger											logger					= LogManager.getLogger(CurrencyExchangeRateManager.class);

	/**
	 * Get the Current Exchange rate per Unit
	 * 
	 * Check the Mapping already cached or not , if it is cached then check the date , if the date is older then 1 day then pull the current exchange 
	 * rate from trade service. Otherwise return the value.
	 * 
	 * @param cumulationCurrency
	 * @param headerCurrencyCode
	 * @return
	 */
	public Double getExchangeRate(String cumulationCurrency, String headerCurrencyCode) 
	{
		if(cumulationCurrency == null || cumulationCurrency.isEmpty() || headerCurrencyCode == null || headerCurrencyCode.isEmpty())
		{
			MessageFormatter.error(logger, "getExchangeRate", null, "Error while calcuating Excehange Rate Source Country Code [{0}]: Target Country code [{1}]", cumulationCurrency, headerCurrencyCode);
			return 0.0;
		}

		try {
			CountryMapping countryMapping = new CountryMapping( cumulationCurrency,  headerCurrencyCode);

			Rate aCurrentRate = m_ExchangeRateCache.get(countryMapping);
			if(aCurrentRate == null || isOldRate(aCurrentRate))
			{
				Double currentExchangeRate = Env.getSingleton().getTradeQualtxClient().getExchangeRate(cumulationCurrency,  headerCurrencyCode);
				if(currentExchangeRate == null)
					return 0.0;
				aCurrentRate = addToCache(countryMapping,currentExchangeRate);
			}
			return aCurrentRate.exchangeRate;
		}
		catch(Exception exec)
		{
			MessageFormatter.error(logger, "getExchangeRate", exec, "Error while calcuating Excehange Rate Source Country Code [{0}]: Target Country code [{1}]", cumulationCurrency, headerCurrencyCode);
		}
		return 0.0;
	}
	/**
	 * Add the exchange rate in cache so that it will resue.
	 * 
	 * It will crate a object of Rate and set the exchange rate with current date.
	 * 
	 * @param countryMapping
	 * @param aCurrentRate
	 * @return
	 */
	private Rate addToCache(CountryMapping countryMapping, Double aCurrentRate) 
	{
		Rate rate = new Rate();
		rate.exchangeRate = aCurrentRate;
		rate.rateOnDate = Calendar.getInstance().getTime();
		m_ExchangeRateCache.put(countryMapping, rate);
		return rate;
	}

	/**
	 * Check whether the Rate in Cache is Older then Current Date
	 * 
	 * @param aCurrentRate
	 * @return
	 */
	private boolean isOldRate(Rate aCurrentRate) {
		Date rateOnDate = aCurrentRate.rateOnDate;
		SimpleDateFormat sdf = new SimpleDateFormat("dd-mm-yyyy");
		return  !sdf.format(rateOnDate).equals(sdf.format(new Date(System.currentTimeMillis())));
	}

	public synchronized void flushCache()
	{
		m_ExchangeRateCache.clear();
	}
	
	
	/**
	 * Custom class to manage the Sour Country to Target Country Exchange Rate
	 * 
	 * @author m2249
	 *
	 */
	private class CountryMapping
	{
		String sourceCountry;
		String targetCountry;
		
		public CountryMapping (String sourceCountry,String targetCountry)
		{
			this.sourceCountry = sourceCountry;
			this.targetCountry = targetCountry;
		}
		
		@Override
		public boolean equals(Object countryMapping)
		{
			if(countryMapping == null)
				return false;
			
			if(! (countryMapping instanceof CountryMapping) ) return false;
			
			CountryMapping secondCtryMapping = (CountryMapping) countryMapping;
			if(this.sourceCountry.equals(secondCtryMapping.sourceCountry)
					&& this.targetCountry.equals(secondCtryMapping.targetCountry))
				return true;
			
			else 
				return false;
		}
		@Override
		public int hashCode()
		{
			return Objects.hash(this.sourceCountry,this.targetCountry);
		}
	}
	
	/**
	 * Rate Object to Store current Exchange rate with current date
	 * 
	 * @author m2249
	 *
	 */
	private class Rate{
		Double exchangeRate;
		Date rateOnDate;
	}
}
