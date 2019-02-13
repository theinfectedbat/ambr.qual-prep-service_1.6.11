package com.ambr.gtm.fta.qts;

public class TrackerLoaderQueries
{
	 public static String qtxTrackerQuery = "SELECT QWS.QTX_WID, QW.BOM_KEY, QWD.QUALTX_KEY, QWS.STATUS, QWD.ANALYSIS_METHOD, QWD.COMPONENTS, QWD.WAIT_FOR_NEXT_ANALYSIS_METHOD,QW.PRIORITY "
	    		+ " FROM AR_QTX_WORK_STATUS QWS"
	    		+ " JOIN AR_QTX_WORK QW"
	    		+ " ON QW.QTX_WID = QWS.QTX_WID"
	    		+ " JOIN AR_QTX_WORK_DETAILS QWD"
	    		+ " ON QWD.QTX_WID = QWS.QTX_WID"
	    		+ " WHERE QWS.status  < ?"
	    		+ " AND QWS.status NOT IN (?,?)";
	 
	 public static String qtxStatusUpdateQuery = "update AR_QTX_WORK_STATUS set status = ?, time_stamp = ? where qtx_wid = ?";
	
	 public static String qtxTrackerReloadQuery = "SELECT QWS.QTX_WID, QW.BOM_KEY, QWD.QUALTX_KEY, QWS.STATUS, QWD.ANALYSIS_METHOD, QWD.COMPONENTS, QWD.WAIT_FOR_NEXT_ANALYSIS_METHOD, QW.PRIORITY "
	    		+ " FROM AR_QTX_WORK_STATUS QWS"
	    		+ " JOIN AR_QTX_WORK QW"
	    		+ " ON QW.QTX_WID = QWS.QTX_WID"
	    		+ " JOIN AR_QTX_WORK_DETAILS QWD"
	    		+ " ON QWD.QTX_WID = QWS.QTX_WID"
	    		+ " WHERE ::TO_REPLACE:: ";
	 
	 public static String ftaCtryDataGroupLoadQueryForOrg=  "SELECT ORG_CODE,GROUP_VALUE,GROUP_TYPE from MDI_DATAGROUP_TYPE_VALUE "
	 		                                               + "WHERE GROUP_NAME = 'FTA_CTRY' AND ORG_CODE = ?";
	 
	 
	 public static String ftaCtryDataGroupLoadQuery=  "SELECT ORG_CODE,GROUP_VALUE,GROUP_TYPE from MDI_DATAGROUP_TYPE_VALUE "
                                                      + "WHERE GROUP_NAME = 'FTA_CTRY'";
	 
	 
	 public static String fullCacheLoadQuery =  "SELECT CONFIG, NODE_CODE FROM MDI_APP_CONFIG "
	 		                                      + "WHERE ACTIVE='Y' AND CONFIG_NAME = 'QE_CONFIG'";
	 public static String orgQEConfigCache =  "SELECT CONFIG, NODE_CODE FROM MDI_APP_CONFIG "
              + "WHERE ACTIVE='Y' AND CONFIG_NAME = 'QE_CONFIG' AND NODE_CODE = ?";
	 
	 
	 
	 public static String fullCachePerORgLoadQuery =  "SELECT CONFIG, NODE_CODE FROM MDI_APP_CONFIG "
	 		                                      + "WHERE CONFIG_NAME = 'QE_CONFIG' AND NODE_CODE = ?";
	 
	 
	 public static String parentOrgLoadQuery = "SELECT PARENT_ORG_CODE FROM MDI_ORG"
	 		                                     + " WHERE PARENT_ORG_CODE <> ORG_CODE AND ORG_CODE= ?";
	 
}
