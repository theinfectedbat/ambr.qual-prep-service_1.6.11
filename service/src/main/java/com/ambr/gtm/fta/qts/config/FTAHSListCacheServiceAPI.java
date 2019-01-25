package com.ambr.gtm.fta.qts.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class FTAHSListCacheServiceAPI
{
	@Autowired
	FTAHSListCache					ftaHsCache;
	
	public static final String	FLUSH_CACHE	= "/qts/config/flush_ftahs_cache";

	@RequestMapping(value = FLUSH_CACHE, method = RequestMethod.GET)
	public ResponseEntity<String> flushCache() throws Exception
	{
		ftaHsCache.flushFTAListCache();
		return new ResponseEntity<String>(HttpStatus.OK);
	}
}
