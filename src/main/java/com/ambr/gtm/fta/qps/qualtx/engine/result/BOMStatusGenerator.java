package com.ambr.gtm.fta.qps.qualtx.engine.result;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.NoSuchElementException;

import org.apache.commons.lang.time.DurationFormatUtils;

import com.ambr.gtm.fta.qps.qualtx.engine.TypedPersistenceQueue;
import com.ambr.platform.utils.misc.ParameterizedMessageUtility;
import com.ambr.platform.utils.queue.TaskQueueProgressSummary;
import com.ambr.platform.utils.queue.TaskQueueThroughputUtility;


/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
class QueueProgressRecord
{
	public String 						queueName;
	public String						threadCount;
	public String						cumulativeTotalWorkItems;
	public String						cumulativeCompletedWorkItems;
	public String						currentWorkItemsSubmittedCount;
	public String						currentWorkItemsInProgressCount;
	public String						bomRelatedWorkItemsSubmittedCount;
	public String						bomRelatedWorkItemsInProgressCount;
	public String						workPositionOldest;
	public String						workPositionNewest;
	public String						throughput;
	public String						duration;
	public TaskQueueProgressSummary		progressSummary;
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theSummary
     *************************************************************************************
     */
	public QueueProgressRecord(TaskQueueProgressSummary theSummary)
		throws Exception
	{
		this.progressSummary = theSummary;
		this.queueName = theSummary.queueName;
		this.threadCount 						= MessageFormat.format("{0,number}", theSummary.threadCount);
		this.cumulativeTotalWorkItems 			= MessageFormat.format("{0}", theSummary.totalWorkItemCount);
		this.cumulativeCompletedWorkItems 		= MessageFormat.format("{0}", theSummary.totalWorkItemCompletedCount);
		this.currentWorkItemsSubmittedCount 	= MessageFormat.format("{0}", theSummary.totalSubmittedWorkItemCount);
		this.currentWorkItemsInProgressCount 	= MessageFormat.format("{0}", theSummary.totalInProgressWorkItemCount);
		this.bomRelatedWorkItemsSubmittedCount 	= MessageFormat.format("{0}", theSummary.filteredSubmittedWorkItemCount);
		this.bomRelatedWorkItemsInProgressCount = MessageFormat.format("{0}", theSummary.filteredInProgressWorkItemCount);
		this.workPositionOldest 				= MessageFormat.format("{0}", theSummary.filteredMinSubmittedPosInQueue);
		this.workPositionNewest 				= MessageFormat.format("{0}", theSummary.filteredMaxSubmittedPosInQueue);
		this.throughput 						= MessageFormat.format("{0,number,0.##}", theSummary.throughput);
		if (theSummary.throughput == 0) {
			this.duration = "unknown";
		}
		else {
			this.duration = DurationFormatUtils.formatDurationHMS(
				(long)((((double)theSummary.filteredMaxSubmittedPosInQueue)/theSummary.throughput) * 1000)
			);
		}
	}
}

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
class BOMStatusGenerator 
{
	QualTXUniversePreparationProgressManager		qtxPrepProgressMgr;
	long											bomKey;
	ParameterizedMessageUtility						msgUtil;
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theQTXPrepProgressMgr
     * @param	theBOMKey
     *************************************************************************************
     */
	public BOMStatusGenerator(QualTXUniversePreparationProgressManager theQTXPrepProgressMgr, long theBOMKey)
		throws Exception
	{
		this.qtxPrepProgressMgr = theQTXPrepProgressMgr;
		this.bomKey = theBOMKey;
		this.msgUtil = new ParameterizedMessageUtility();
	}
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public String generate()
		throws Exception
	{
		int									aMaxQueueNameLength;
		String								aMaxDuration;
		ParameterizedMessageUtility			aUtil = new ParameterizedMessageUtility();
		ArrayList<QueueProgressRecord>		aList;
		String								aFirstLine;
		long								aRemainingWorkItemsCount;
		
		this.qtxPrepProgressMgr.getTrackerStatusSummary(aUtil);

		aList = this.getProgressSummaryList();
		aMaxQueueNameLength = aList.stream().mapToInt(aRecord->aRecord.queueName.length()).max().getAsInt();
		aRemainingWorkItemsCount = aList.stream().mapToLong(
			(aRecord)->
				aRecord.progressSummary.filteredInProgressWorkItemCount +
				aRecord.progressSummary.filteredSubmittedWorkItemCount
		).sum();
		
		try {
			aMaxDuration = aList.stream().filter(aRec->!aRec.duration.equalsIgnoreCase("unknown")).max(
				new Comparator<QueueProgressRecord>()
				{
					@Override
					public int compare(QueueProgressRecord theRec1, QueueProgressRecord theRec2) 
					{
						return theRec1.duration.compareTo(theRec2.duration);
					}
				}
			).get().duration;
		}
		catch (NoSuchElementException e) {
			aMaxDuration = "unknown";
		}

		aFirstLine = MessageFormat.format("Queue Name{0}|Threads|Total       |Completed   |Current    |Current      |BOM-related|BOM-related  |Work    |Work    |Throughput |Approx.", 
			aUtil.createPaddingString("Queue Name", ' ', aMaxQueueNameLength+1)
		);
		
		aUtil.format("{0}", false, true, aUtil.createPaddingString("", '_', aFirstLine.length()+5));
		aUtil.format("BOM [{0,number,#}] Status [{1}] {2}", false, true, 
			this.bomKey,
			(aRemainingWorkItemsCount == 0)? "COMPLETED" : "IN PROGRESS",
			(aRemainingWorkItemsCount == 0)? "" : MessageFormat.format("Approx. Duration [{0}]", aMaxDuration)
		);
		
		aUtil.format("{0}", false, true, aUtil.createPaddingString("", '_', aFirstLine.length()+5));
		aUtil.format(aFirstLine, false, true);
		aUtil.format("{0}|       |Work Items  |Work Items  |Work Items |Work Items   |work items |work items   |Position|Position|(tasks/sec)|Duration", false, true,
			aUtil.createPaddingString("", ' ', aMaxQueueNameLength+1)
		);
		aUtil.format("{0}|       |(cumulative)|(cumulative)|(submitted)|(in-progress)|(submitted)|(in-progress)|(oldest)|(newest)|           |", false, true,
			aUtil.createPaddingString("", ' ', aMaxQueueNameLength+1)
		);
		aUtil.format("{0}|{1}|{2}|{3}|{4}|{5}|{6}|{7}|{8}|{9}|{10}|{11}", false, true,
			aUtil.createPaddingString("", '_', aMaxQueueNameLength+1),
			aUtil.createPaddingString("", '_', 7),
			aUtil.createPaddingString("", '_', 12),
			aUtil.createPaddingString("", '_', 12),
			aUtil.createPaddingString("", '_', 11),
			aUtil.createPaddingString("", '_', 13),
			aUtil.createPaddingString("", '_', 11),
			aUtil.createPaddingString("", '_', 13),
			aUtil.createPaddingString("", '_', 8),
			aUtil.createPaddingString("", '_', 8),
			aUtil.createPaddingString("", '_', 11),
			aUtil.createPaddingString("", '_', 12)
		);
		
		for (QueueProgressRecord aProgressRecord : aList) {
			
			aUtil.format("[{0}{1}]{2}[{3}]{4}[{5}]{6}[{7}]{8}[{9}]{10}[{11}]{12}[{13}]{14}[{15}]{16}[{17}]{18}[{19}]{20}[{21}] {22}", false, true, 
				aProgressRecord.queueName, 								
				aUtil.createPaddingString(aProgressRecord.queueName, ' ', aMaxQueueNameLength),
				
				aUtil.createPaddingString(aProgressRecord.threadCount, ' ', 6),
				aProgressRecord.threadCount,
				
				aUtil.createPaddingString(aProgressRecord.cumulativeTotalWorkItems, ' ', 11),
				aProgressRecord.cumulativeTotalWorkItems,
				
				aUtil.createPaddingString(aProgressRecord.cumulativeCompletedWorkItems, ' ', 11),
				aProgressRecord.cumulativeCompletedWorkItems,
				
				aUtil.createPaddingString(aProgressRecord.currentWorkItemsSubmittedCount, ' ', 10),
				aProgressRecord.currentWorkItemsSubmittedCount,
				
				aUtil.createPaddingString(aProgressRecord.currentWorkItemsInProgressCount, ' ', 12),
				aProgressRecord.currentWorkItemsInProgressCount,
				
				aUtil.createPaddingString(aProgressRecord.bomRelatedWorkItemsSubmittedCount, ' ', 10),
				aProgressRecord.bomRelatedWorkItemsSubmittedCount,
				
				aUtil.createPaddingString(aProgressRecord.bomRelatedWorkItemsInProgressCount, ' ', 12),
				aProgressRecord.bomRelatedWorkItemsInProgressCount,
				
				aUtil.createPaddingString(aProgressRecord.workPositionOldest, ' ', 7),
				aProgressRecord.workPositionOldest,
				
				aUtil.createPaddingString(aProgressRecord.workPositionNewest, ' ', 7),
				aProgressRecord.workPositionNewest,
				
				aUtil.createPaddingString(aProgressRecord.throughput, ' ', 10),
				aProgressRecord.throughput,

				aProgressRecord.duration
			);
		}
		
		return aUtil.getMessage();
	}
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	private ArrayList<QueueProgressRecord> getProgressSummaryList()
		throws Exception
	{
		QueueProgressRecord				aProgressRecord;
		ArrayList<QueueProgressRecord>	aList;
		TaskQueueThroughputUtility		aUtil;
		
		aUtil = new TaskQueueThroughputUtility();
		aUtil.addQueue(this.qtxPrepProgressMgr.queueUniverse.bomQueue);
		aUtil.addQueue(this.qtxPrepProgressMgr.queueUniverse.tradeLaneQueue);
		aUtil.addQueue(this.qtxPrepProgressMgr.queueUniverse.compQueue);
		aUtil.addQueue(this.qtxPrepProgressMgr.queueUniverse.classificationQueue);
		aUtil.addQueue(this.qtxPrepProgressMgr.queueUniverse.compIVAPullQueue);
		aUtil.addQueue(this.qtxPrepProgressMgr.queueUniverse.persistenceRetryQueue);
		
		aUtil.addQueues(this.qtxPrepProgressMgr.queueUniverse.qualTXQueue.getInternalQueues());
		aUtil.addQueues(this.qtxPrepProgressMgr.queueUniverse.qualTXPriceQueue.getInternalQueues());
		aUtil.addQueues(this.qtxPrepProgressMgr.queueUniverse.qualTXComponentQueue.getInternalQueues());
		aUtil.addQueues(this.qtxPrepProgressMgr.queueUniverse.qualTXComponentPriceQueue.getInternalQueues());
		
		for (TypedPersistenceQueue<?> aQueue : this.qtxPrepProgressMgr.queueUniverse.qualTXdataExtQueue.getInternalQueues()) {
			aUtil.addQueues(aQueue.getInternalQueues());
		}

		for (TypedPersistenceQueue<?> aQueue : this.qtxPrepProgressMgr.queueUniverse.qualTXComponentdataExtQueue.getInternalQueues()) {
			aUtil.addQueues(aQueue.getInternalQueues());
		}

		aUtil.measureThroughput(5);
		aUtil.collectProgressSummary(MessageFormat.format("{0,number,#}", this.bomKey));
		
		aList = new ArrayList<>();
		for (TaskQueueProgressSummary aSummary : aUtil.getProgressSummaryList()) {
			aProgressRecord = new QueueProgressRecord(aSummary);
			aList.add(aProgressRecord);
		}
		
		aList.sort(
			new Comparator<QueueProgressRecord>()
			{
				@Override
				public int compare(QueueProgressRecord theRec1, QueueProgressRecord theRec2) 
				{
					return theRec1.queueName.compareToIgnoreCase(theRec2.queueName);
				}
			}
		);
		
		return aList;
	}
}
