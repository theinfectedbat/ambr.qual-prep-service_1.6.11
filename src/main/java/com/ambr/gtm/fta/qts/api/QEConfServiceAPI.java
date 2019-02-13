package com.ambr.gtm.fta.qts.api;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.ambr.gtm.fta.qts.config.FTAHSListCache;
import com.ambr.gtm.fta.qts.config.QEConfigCache;

@RestController
public class QEConfServiceAPI
{

	public static final String	FTA_COI_CONFIG	= "/qts/api/fta_coi_config";

	public static final String	HS_EXCEP_LIST	= "qts/api/qualtx/hs/excep_list";

	@Autowired
	private QEConfigCache		qeConfigCache;

	@Autowired
	FTAHSListCache		ftaHSListCache;

	public QEConfServiceAPI() throws Exception
	{

	}

	@RequestMapping(value = FTA_COI_CONFIG, method = RequestMethod.POST)
	public ResponseEntity<String> flushQEConfig(@RequestParam(name = "org_code", required = true) String ORG_CODE) throws Exception

	{
		qeConfigCache.flushQEConfigCache(ORG_CODE);
		return new ResponseEntity<String>(HttpStatus.OK);
	}

	public static String GetURLPath() throws Exception
	{
		return FTA_COI_CONFIG;
	}

	@RequestMapping(value = HS_EXCEP_LIST, method = RequestMethod.GET)
	public ResponseEntity<String> flushQEConfig() throws Exception

	{
		ftaHSListCache.flushFTAListCache();
		return new ResponseEntity<String>(HttpStatus.OK);
	}

}
