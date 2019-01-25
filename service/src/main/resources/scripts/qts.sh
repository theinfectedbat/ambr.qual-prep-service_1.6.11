java -Xmx1024m -Xms1024m -jar target/ambr.qual-prep-service-1.0.0.jar \
	--proc=QTS \
	--server.port=8242 \
	\
	--com.ambr.gtm.fta.qts.requal_service_start=N \
	--com.ambr.gtm.fta.qts.tracker_service_start=Y \
	--com.ambr.gtm.fta.qts.initial_load_required=Y \
	\
	--com.ambr.platform.rdbms.primary-ds-cfg.enabled_flag=Y \
	--com.ambr.platform.rdbms.primary-ds-cfg.datasource.url=jdbc:oracle:thin:@bomd.cu1fummwramm.us-east-1.rds.amazonaws.com:20189:BOMD \
	--com.ambr.platform.rdbms.primary-ds-cfg.datasource.username=TA_1569_OWNER \
	--com.ambr.platform.rdbms.primary-ds-cfg.target-schema=TA_1569_OWNER \
	--com.ambr.platform.rdbms.primary-ds-cfg.datasource.password=TA_1569_OWNER \
	--com.ambr.platform.rdbms.primary-ds-cfg.datasource.max-active=100 \
	\
	--com.ambr.gtm.fta.qps.qualtx.engine.api.GenerateUniverseClientAPI.url=http://localhost:8380 \
	--com.ambr.gtm.fta.qps.bom.api.BOMUniverseRefreshClientAPI.url=http://localhost:8380 \
	--com.ambr.gtm.fta.qps.gpmclaimdetail.api.GPMClaimDetailsUniverseRefreshClientAPI.url=http://localhost:8380 \
	--com.ambr.gtm.fta.qps.gpmclass.api.GPMClassificationUniverseRefreshClientAPI.url=http://localhost:8380 \
	--com.ambr.gtm.fta.qps.gpmsrciva.api.GPMSourceIVAUniverseRefreshClientAPI.url=http://localhost:8380 \
	--com.ambr.gtm.fta.qps.qualtx.universe.api.QualTXDetailUniverseRefreshClientAPI.url=http://localhost:8380 \
	\
	--com.ambr.gtm.fta.qts.api.ta.serviceURL=http://localhost:7566/TA/service
