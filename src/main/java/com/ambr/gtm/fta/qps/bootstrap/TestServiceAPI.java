package com.ambr.gtm.fta.qps.bootstrap;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;

import javax.sql.DataSource;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.ambr.gtm.fta.qps.bom.qualstatus.BOMQualificationStatus;
import com.ambr.gtm.fta.qps.bom.qualstatus.BOMQualificationStatusGenerator;
import com.ambr.gtm.fta.qps.qualtx.engine.PreparationEngineQueueUniverse;
import com.ambr.gtm.fta.qps.qualtx.engine.QualTX;
import com.ambr.gtm.fta.qps.qualtx.engine.QualTXComponent;
import com.ambr.gtm.fta.qps.qualtx.engine.QualTXComponentDataExtension;
import com.ambr.gtm.fta.qps.qualtx.engine.QualTXPrice;
import com.ambr.gtm.utils.legacy.rdbms.de.DataExtensionConfigurationRepository;
import com.ambr.gtm.utils.legacy.sps.SimplePropertySheet;
import com.ambr.gtm.utils.legacy.sps.SimplePropertySheetManager;
import com.ambr.platform.rdbms.bootstrap.PrimaryDataSourceConfiguration;
import com.ambr.platform.rdbms.bootstrap.SchemaDescriptorService;
import com.ambr.platform.rdbms.orm.DataRecordColumnModification;
import com.ambr.platform.rdbms.orm.DataRecordModificationTracker;
import com.ambr.platform.rdbms.orm.EntityManager;
import com.ambr.platform.rdbms.orm.EntityModificationTracker;
import com.ambr.platform.rdbms.orm.EntitySaver;
import com.ambr.platform.rdbms.orm.exception.EntityDoesNotExistException;
import com.ambr.platform.rdbms.schema.SchemaDescriptor;
import com.ambr.platform.rdbms.util.resultsetqueue.ResultSetMaxIterationsExceededException;
import com.ambr.platform.rdbms.util.resultsetqueue.SQLQueryResultRow;
import com.ambr.platform.rdbms.util.resultsetqueue.SQLQueryResultRowHandlerInterface;
import com.ambr.platform.rdbms.util.resultsetqueue.SQLQueryResultSetProcessor;
import com.ambr.platform.uoid.UniversalObjectIDGenerator;
import com.ambr.platform.utils.log.MessageFormatter;
import com.ambr.platform.utils.log.PerformanceTracker;
import com.ambr.platform.utils.log.api.AllLoggersDetail;
import com.ambr.platform.utils.log.api.SpringActuatorLoggersClientAPI;
import com.ambr.platform.utils.propertyresolver.ConfigurationPropertyResolver;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
@RestController
public class TestServiceAPI 
{
	static Logger			logger = LogManager.getLogger(TestServiceAPI.class);
	
	@Autowired private SpringActuatorLoggersClientAPI		loggerAPI;
	@Autowired private DataSource							dataSrc;
	@Autowired private PlatformTransactionManager			txMgr;
	@Autowired private ConfigurationPropertyResolver		propertyResolver;
	@Autowired private SimplePropertySheetManager			propSheetMgr;
	@Autowired private SchemaDescriptorService				schemaDescService;
	@Autowired private DataExtensionConfigurationRepository	repos;
	@Autowired private UniversalObjectIDGenerator			idGenerator;
	@Autowired private PreparationEngineQueueUniverse		queueUniverse;
	private int rowNum;
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	@RequestMapping("/test")
	public void execute()
		throws Exception
	{
		this.test9();
	}
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	@RequestMapping("/test/measure_query_performance")
	public String measureQueryPerformance
		(
			@RequestParam(name = "target_schema", 	required = false) 	String theUserProvidedTargetSchema,
			@RequestParam(name = "fetch_size", 		required = false) 	Integer theFetchSize,
			@RequestParam(name = "sql_query", 		required = true) 	String theSqlText
		)
		throws Exception
	{
		PerformanceTracker		aPerfTracker = new PerformanceTracker(logger, Level.INFO, "measureQueryPerformance");
		JdbcTemplate			aTemplate = new JdbcTemplate(this.dataSrc);
		String 					aTargetSchema;
		String					aMsg;
		
		aPerfTracker.start();
		
		this.rowNum = 0;
		
		try {
			if (theFetchSize == null) {
				theFetchSize = 1000;
			}
			
			aTemplate.setFetchSize(theFetchSize);
			
			if (theUserProvidedTargetSchema == null) {
				aTargetSchema = this.propertyResolver.getPropertyValue(PrimaryDataSourceConfiguration.PROPERTY_NAME_PRIMARY_DATA_SOURCE_CFG_TARGET_SCHEMA);
				if (aTargetSchema != null) {
					MessageFormatter.info(logger, "measureQueryPerformance", "setting target schema [{0}]", theUserProvidedTargetSchema);
					aTemplate.execute(MessageFormat.format("alter session set current_schema = {0}", theUserProvidedTargetSchema));
				}
			}
			else {
				aTargetSchema = theUserProvidedTargetSchema;
			}
			
			if (aTargetSchema != null) {
				MessageFormatter.info(logger, "measureQueryPerformance", "setting target schema [{0}]", aTargetSchema);
				aTemplate.execute(MessageFormat.format("alter session set current_schema = {0}", aTargetSchema));
			}

			aTemplate.query(
				theSqlText,
				new RowCallbackHandler()
				{
					@Override
					public void processRow(ResultSet arg0) 
						throws SQLException 
					{
						TestServiceAPI.this.rowNum++;
					}
				}
			);
		}
		finally {
			aMsg = aPerfTracker.stop("Record Count [{0}]", new Object[]{this.rowNum});
		}
		
		return aMsg;
	}
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	private void test1()
		throws Exception
	{
		AllLoggersDetail	aAllLoggersDetail;
		
		aAllLoggersDetail = this.loggerAPI.getAllLoggers(null);
		System.out.println("done");
	}
	
	private void test2()
		throws Exception
	{
		SQLQueryResultSetProcessor	aProcessor;
		
		try {
			aProcessor = new SQLQueryResultSetProcessor();
			aProcessor.setMaxQueueDepth(5000);
			aProcessor.setThreadCount(10);
//			aProcessor.setMaxRowIterations(10);
			aProcessor.execute(
				"select * from mdi_bom_comp", 
				null, 
				this.dataSrc, 
				new SQLQueryResultRowHandlerInterface() 
				{
					@Override
					public void handleRow(SQLQueryResultRow theRow) 
						throws Exception 
					{
						MessageFormatter.info(logger, "test2", "BOM [{0}] Row [{1}] processed", theRow.getValue("bom_id"), theRow.rowNum);
					}
				}
			);
		}
		catch (ResultSetMaxIterationsExceededException e) {
			MessageFormatter.error(logger, "test2", e, "BOM [{0}] processed");
		}
	}
	
	public void test3()
		throws Exception
	{
		DataExtensionConfigurationRepository	aRepos;
		
		aRepos = new DataExtensionConfigurationRepository();
		aRepos.load(new JdbcTemplate(this.dataSrc), 1000, null);
		
		System.out.println("done");
	}
	
	public void test4()
		throws Exception
	{
		SimplePropertySheet	aSheet;
		
		aSheet = this.propSheetMgr.getPropertySheet("SYSTEM", "FTA_HS_CONFIGURATION_LEVEL");
		System.out.println();
	}
	
	public void test5()
		throws Exception
	{
		QualTX									aQualTX;
		QualTXComponent							aQualTXComp;
		EntityModificationTracker<QualTX>		aTracker;
		EntitySaver								aEntitySaver;
		JdbcTemplate							aTemplate = new JdbcTemplate(this.dataSrc);
		SchemaDescriptor						aSchemaDesc = this.schemaDescService.getPrimarySchemaDescriptor();
		
		aQualTX = new QualTX();
		aQualTX.alt_key_qualtx = 1L;
		aQualTX.tx_id = MessageFormat.format("{0,number,#}", aQualTX.alt_key_qualtx);
		aQualTX.org_code = "TEST";
		aQualTX.ctry_of_import = "US";
		aQualTX.fta_code = "NAFTA";

		aQualTXComp = aQualTX.createComponent();
		aQualTXComp.area = 2.3;
		aQualTXComp.description = "COMPUTER";
		
		aTracker = new EntityModificationTracker<QualTX>(QualTX.class);
		aTracker.startTracking(aQualTX);
		aTracker.stopTracking();
		
		System.out.println(MessageFormat.format("1. New records [{0}]", aTracker.getNewRecords().size()));
		System.out.println(MessageFormat.format("1. Modified records [{0}]", aTracker.getModifiedRecords().size()));
		System.out.println(MessageFormat.format("1. Deleted records [{0}]", aTracker.getDeletedRecords().size()));
		System.out.println();
		
		aEntitySaver = new EntitySaver(aTracker, this.txMgr, aSchemaDesc, aTemplate, "SYSTEM");
		aEntitySaver.execute();
		
		aTracker.startTracking();
		aQualTX.alt_key_qualtx = new Long(1);
		aQualTX.ctry_of_import = MessageFormat.format("{0}", "USA");
		aQualTX.fta_code = "NAFTA_MX";
		aQualTXComp.area = 2.3;

		aTracker.stopTracking();
		System.out.println(MessageFormat.format("2. New records [{0}]", aTracker.getNewRecords().size()));
		System.out.println(MessageFormat.format("2. Modified records [{0}]", aTracker.getModifiedRecords().size()));
		
		for (DataRecordModificationTracker<?> aRecordTracker : aTracker.getModifiedRecords()) {
			
			for (DataRecordColumnModification aColMod : aRecordTracker.getColumnModifications()) {
				System.out.print("2.   ");
				System.out.println(aColMod.toString());
			}
		}
		
		System.out.println(MessageFormat.format("2. Deleted records [{0}]", aTracker.getDeletedRecords().size()));
		System.out.println();

		aEntitySaver = new EntitySaver(aTracker, this.txMgr, aSchemaDesc, aTemplate, "SYSTEM");
		aEntitySaver.execute();
		
		aTracker.startTracking();
		aQualTX.compList.remove(0);

		aTracker.stopTracking();
		System.out.println(MessageFormat.format("3. New records [{0}]", aTracker.getNewRecords().size()));
		System.out.println(MessageFormat.format("3. Modified records [{0}]", aTracker.getModifiedRecords().size()));
		System.out.println(MessageFormat.format("3. Deleted records [{0}]", aTracker.getDeletedRecords().size()));
		System.out.println();

		aEntitySaver = new EntitySaver(aTracker, this.txMgr, aSchemaDesc, aTemplate, "SYSTEM");
		aEntitySaver.execute();
	}
	
	public void test6()
		throws Exception
	{
		QualTX						aQualTX;
		QualTXComponent				aQualTXComp;
		EntityManager<QualTX>		aQualTXMgr;
		
		aQualTXMgr = new EntityManager<>(
			QualTX.class,
			this.txMgr, 
			this.schemaDescService.getPrimarySchemaDescriptor(), 
			new JdbcTemplate(this.dataSrc)
		);
		
		aQualTXMgr.getLoader().setTableFilter(new String[]{"mdi_qualtx_comp"});
		
		// Only select the specified columns.  Note that all primary key columns will always get selected.
		// Additionally, since the QualTXKey object specifies alt_key_qualtx as it's key column, that column
		// will also get automatically selected.  In this particular instance, a total of 9 columns will get 
		// selected, TX_ID, ORG_CODE, ALT_KEY_QUALTX, and the 6 columns specified below.
		aQualTXMgr.getLoader().setColumnFilter(
			"mdi_qualtx", 
			new String[]{"fta_code", "src_key", "ctry_of_import", "iva_code", "effective_from", "effective_to"}
		);
		
		try {
			aQualTX = aQualTXMgr.loadExistingEntity(new QualTX(-8905056134584405263L));
		}
		catch (EntityDoesNotExistException e) {
			// no record found in the database
			throw e;
		}
		
		// modify two fields of the header record
		aQualTX.ctry_of_import = MessageFormat.format("{0}", "USA");
		aQualTX.fta_code = "NAFTA_MX";
		
		// modify 1 field of a particular component record
		aQualTXComp = aQualTX.compList.get(0);
		aQualTXComp.area += 2.3;
		
		// delete a particular component by removing it from the component array list
//		aQualTX.compList.remove(1);
		
		// inspect the changes that have occurred
		
		System.out.println(MessageFormat.format("1. New records [{0}]", aQualTXMgr.getTracker().getNewRecords().size()));
		System.out.println(MessageFormat.format("1. Modified records [{0}]", aQualTXMgr.getTracker().getModifiedRecords().size()));
		System.out.println(MessageFormat.format("1. Deleted records [{0}]", aQualTXMgr.getTracker().getDeletedRecords().size()));

		for (DataRecordModificationTracker<?> aRecordTracker : aQualTXMgr.getTracker().getModifiedRecords()) {
			for (DataRecordColumnModification aColMod : aRecordTracker.getColumnModifications()) {
				System.out.print("1.   ");
				System.out.println(aColMod.toString());
			}
		}
		
		// all changes will be persisted to the database
		aQualTXMgr.save();
		
		// modify 2 fields of the header again
		aQualTX.ctry_of_import = MessageFormat.format("{0}", "USA");
		aQualTX.fta_code = "NAFTA_MXXXX";

		// only the header record is updated
		aQualTXMgr.save();
		
		System.out.println("done");
	}
	
	public void test7()
		throws Exception
	{
		QualTX						aQualTX;
		QualTX						aQualTXKey = new QualTX(-8077345849906719460L);
		QualTXComponent				aQualTXComp;
		EntityManager<QualTX>		aQualTXMgr;

		// create an EntityManager and load the MDI_QUALTX and MDI_QUALTX_COMP tables 
		aQualTXMgr = new EntityManager<>(QualTX.class, this.txMgr, this.schemaDescService.getPrimarySchemaDescriptor(),	new JdbcTemplate(this.dataSrc));
		aQualTXMgr.getLoader().setTableFilter(new String[]{"mdi_qualtx_comp"});
		
		try {
			aQualTX = aQualTXMgr.loadExistingEntity(aQualTXKey);
		}
		catch (EntityDoesNotExistException e) {
			// no record found in the database
			throw e;
		}

		System.out.println(MessageFormat.format("1. MDI_QUALTX records [{0}]", 1));
		System.out.println(MessageFormat.format("1. MDI_QUALTX_COMP records [{0}]", aQualTX.compList.size()));
		System.out.println(MessageFormat.format("1. MDI_QUALTX_PRICE records [{0}]", aQualTX.priceList.size()));
		
		// At this point, we will "manually" load the price table.  The EntityManager will still recognize
		// the price records as "existing" records
		aQualTXMgr.loadTable("mdi_qualtx_price");

		System.out.println(MessageFormat.format("2. MDI_QUALTX records [{0}]", 1));
		System.out.println(MessageFormat.format("2. MDI_QUALTX_COMP records [{0}]", aQualTX.compList.size()));
		System.out.println(MessageFormat.format("2. MDI_QUALTX_PRICE records [{0}]", aQualTX.priceList.size()));
		
		if (aQualTX.priceList.size() > 0) {
			QualTXPrice aPrice = aQualTX.priceList.get(0);
			aPrice.price = aPrice.price * 2;
		}
		
		// Stop tracking so we can inspect the modification history
		aQualTXMgr.getTracker().stopTracking();
		
		System.out.println(MessageFormat.format("3. New records [{0}]", aQualTXMgr.getTracker().getNewRecords().size()));
		System.out.println(MessageFormat.format("3. Modified records [{0}]", aQualTXMgr.getTracker().getModifiedRecords().size()));
		System.out.println(MessageFormat.format("3. Deleted records [{0}]", aQualTXMgr.getTracker().getDeletedRecords().size()));

		for (DataRecordModificationTracker<?> aRecordTracker : aQualTXMgr.getTracker().getModifiedRecords()) {
			for (DataRecordColumnModification aColMod : aRecordTracker.getColumnModifications()) {
				System.out.print("3.   ");
				System.out.println(aColMod.toString());
			}
		}
		
		// all changes will be persisted to the database
		aQualTXMgr.save();
		
		aQualTXMgr.save();
		System.out.println("done");
	}

	public void test8()
		throws Exception
	{
		QualTX							aQualTX;
		QualTX							aQualTXKey = new QualTX(-5780709142468062205L);
		QualTXComponent					aQualTXComp;
		QualTXComponentDataExtension	aQualTXCompDE;
		EntityManager<QualTX>			aQualTXMgr;
	
		// create an EntityManager and load the MDI_QUALTX and MDI_QUALTX_COMP tables 
		aQualTXMgr = new EntityManager<>(QualTX.class, this.txMgr, this.schemaDescService.getPrimarySchemaDescriptor(),	new JdbcTemplate(this.dataSrc));
		aQualTXMgr.getLoader().setTableFilter(new String[]{"mdi_qualtx_comp", "mdi_qualtx_comp_de"});
		
		try {
			aQualTX = aQualTXMgr.loadExistingEntity(aQualTXKey);
			aQualTX.idGenerator = this.idGenerator;
		}
		catch (EntityDoesNotExistException e) {
			// no record found in the database
			throw e;
		}
	
		System.out.println(MessageFormat.format("1. MDI_QUALTX records [{0}]", 1));
		System.out.println(MessageFormat.format("1. MDI_QUALTX_COMP records [{0}]", aQualTX.compList.size()));
		System.out.println(MessageFormat.format("1. MDI_QUALTX_PRICE records [{0}]", aQualTX.priceList.size()));
		
		aQualTXMgr.loadTable("mdi_qualtx_comp_de");
		aQualTXComp = aQualTX.compList.get(0);
		aQualTXCompDE = aQualTXComp.createDataExtension("IMPL_BOM_PROD_FAMILY:TEXTILES", this.repos, this.idGenerator);
		aQualTXCompDE.setValue("flexfield_var1", "TYPE_1");
		aQualTXCompDE.setValue("flexfield_var2", "ORIGINATING_STATUS_1");
		aQualTXCompDE.setValue("flexfield_var3", "US");
		aQualTXCompDE.setValue("flexfield_var4", "MX");
		aQualTXCompDE.setValue("flexfield_var5", "LBS");
		aQualTXCompDE.setValue("flexfield_num1", 123);
		aQualTXCompDE.setValue("flexfield_var7", "KNIT_TO_SHAPE_1");
		
		// Stop tracking so we can inspect the modification history
		aQualTXMgr.getTracker().stopTracking();
		
		System.out.println(MessageFormat.format("3. New records [{0}]", aQualTXMgr.getTracker().getNewRecords().size()));
		System.out.println(MessageFormat.format("3. Modified records [{0}]", aQualTXMgr.getTracker().getModifiedRecords().size()));
		System.out.println(MessageFormat.format("3. Deleted records [{0}]", aQualTXMgr.getTracker().getDeletedRecords().size()));
	
		for (DataRecordModificationTracker<?> aRecordTracker : aQualTXMgr.getTracker().getModifiedRecords()) {
			for (DataRecordColumnModification aColMod : aRecordTracker.getColumnModifications()) {
				System.out.print("3.   ");
				System.out.println(aColMod.toString());
			}
		}
		
		// all changes will be persisted to the database
		aQualTXMgr.save("TEST_USER");
		
		aQualTXMgr.save();
		System.out.println("done");
	}
	
	public void test9()
		throws Exception
	{
		BOMQualificationStatus			aStatus;
		BOMQualificationStatusGenerator	aGenerator;
		
		aGenerator = new BOMQualificationStatusGenerator(this.queueUniverse);
		aStatus = aGenerator.generate(735633486L);
		System.out.println(aStatus.toString());
	}
}
