package com.ambr.gtm.fta.qts.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class FTACtryConfigCacheServiceAPI
{
	@Autowired
	FTACtryConfigCache					ftaCtryCache;
	
	public static final String	FLUSH_CACHE	= "/qts/config/flush_ftactry_cache";

	@RequestMapping(value = FLUSH_CACHE, method = RequestMethod.POST)
	public ResponseEntity<String> flushConfig(@RequestParam(name = "org_code", required = true) String ORG_CODE) throws Exception

	{
		ftaCtryCache.flushCache(ORG_CODE);
		return new ResponseEntity<String>(HttpStatus.OK);
	}
}
