package com.ambr.gtm.fta.qps.util;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class CurrencyExchangeRateManagerServiceAPI
{
	@Autowired
	CurrencyExchangeRateManager					currencyManager;
	
	public static final String	FLUSH_CACHE	= "/qts/util/flush_currency_cache";

	@RequestMapping(value = FLUSH_CACHE, method = RequestMethod.GET)
	public ResponseEntity<String> flushCache() throws Exception
	{
		currencyManager.flushCache();
		return new ResponseEntity<String>(HttpStatus.OK);
	}
}
