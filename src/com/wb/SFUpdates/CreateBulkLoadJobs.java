

/**
 * @author Srinizkumar Konakanchi
 *
 */

package com.wb.SFUpdates;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.poi.openxml4j.exceptions.InvalidFormatException;

import com.sforce.async.AsyncApiException;
import com.sforce.async.BatchInfo;
import com.sforce.async.BatchStateEnum;
import com.sforce.async.BulkConnection;
import com.sforce.async.CSVReader;
import com.sforce.async.ConcurrencyMode;
import com.sforce.async.ContentType;
import com.sforce.async.JobInfo;
import com.sforce.async.JobStateEnum;
import com.sforce.async.OperationEnum;
import com.sforce.ws.ConnectionException;

public class CreateBulkLoadJobs {

	/**
	 * Create a new job using the Bulk API.
	 */
	public JobInfo createInsertJob(String sobjectType, BulkConnection connection) throws AsyncApiException {
		JobInfo job = new JobInfo();
		job.setObject(sobjectType);
		job.setOperation(OperationEnum.insert);
		job.setConcurrencyMode(ConcurrencyMode.Serial);
		job.setContentType(ContentType.CSV);
		job = connection.createJob(job);
		// System.out.println(job);
		return job;
	}

	/**
	 * Create a new job using the Bulk API.
	 */
	public JobInfo createUpdateJob(String sobjectType, BulkConnection connection) throws AsyncApiException {
		JobInfo job = new JobInfo();
		job.setObject(sobjectType);
		job.setOperation(OperationEnum.update);
		job.setContentType(ContentType.CSV);
		job = connection.createJob(job);
		// System.out.println(job);
		return job;
	}

	/**
	 * Create a new job using the Bulk API.
	 */
	public JobInfo createDeleteJob(String sobjectType, BulkConnection connection) throws AsyncApiException {
		JobInfo job = new JobInfo();
		job.setObject(sobjectType);
		job.setOperation(OperationEnum.delete);
		job.setContentType(ContentType.CSV);
		job = connection.createJob(job);
		// System.out.println(job);
		return job;
	}
	
	/**
	 * Create and upload batches using a CSV file. The file into the appropriate
	 * size batch files.
	 * 
	 */
	public BatchInformation createBatchesFromCSVFile(BulkConnection connection,	JobInfo jobInfo, String csvFileName)
					throws IOException, ConnectionException, AsyncApiException, InvalidFormatException {
		List<BatchInfo> batchInfos = new ArrayList<BatchInfo>();
		String aId = "";
		Boolean isSuccessful = false;
		
        BufferedReader rdr = new BufferedReader(
            new InputStreamReader(new FileInputStream(csvFileName))
        );
        // read the CSV header row
        byte[] headerBytes = (rdr.readLine() + "\n").getBytes("UTF-8");
        int headerBytesLength = headerBytes.length;
        File tmpFile = File.createTempFile("bulkAPIInsert", ".csv");

        // Split the CSV file into multiple batches
        try {
            FileOutputStream tmpOut = new FileOutputStream(tmpFile);
            int maxBytesPerBatch = 1000; // 10 million bytes per batch
            int maxRowsPerBatch = 10; // 10 thousand rows per batch
            int currentBytes = 0;
            int currentLines = 0;
            String nextLine;
            while ((nextLine = rdr.readLine()) != null) {
                byte[] bytes = (nextLine + "\n").getBytes("UTF-8");
                // Create a new batch when our batch size limit is reached
                if (currentBytes + bytes.length > maxBytesPerBatch
                  || currentLines > maxRowsPerBatch) {
                    createBatch(tmpOut, tmpFile, batchInfos, connection, jobInfo);
                    currentBytes = 0;
                    currentLines = 0;
                }
                if (currentBytes == 0) {
                    tmpOut = new FileOutputStream(tmpFile);
                    tmpOut.write(headerBytes);
                    currentBytes = headerBytesLength;
                    currentLines = 1;
                }
                tmpOut.write(bytes);
                currentBytes += bytes.length;
                currentLines++;
            }
            // Finished processing all rows
            // Create a final batch for any remaining data
            if (currentLines > 1) {
                createBatch(tmpOut, tmpFile, batchInfos, connection, jobInfo);
            }
        } finally {
            tmpFile.delete();
        }
		return new BatchInformation(batchInfos, aId, isSuccessful);
	}

	/**
	 * Create a batch by uploading the contents of the file. This closes the
	 * output stream.
	 */
	public void createBatch(FileOutputStream tmpOut, File tmpFile, List<BatchInfo> batchInfos,
			BulkConnection connection, JobInfo jobInfo) throws IOException, AsyncApiException {
		tmpOut.flush();
		tmpOut.close();
		FileInputStream tmpInputStream = new FileInputStream(tmpFile);
		try {
			BatchInfo batchInfo = connection.createBatchFromStream(jobInfo, tmpInputStream);
			// System.out.println(batchInfo);
			batchInfos.add(batchInfo);

		} finally {
			tmpInputStream.close();
		}
	}

	/**
	 * Closes the job
	 */
	public void closeJob(BulkConnection connection, String jobId) throws AsyncApiException {
		JobInfo job = new JobInfo();
		job.setId(jobId);
		job.setState(JobStateEnum.Closed);
		connection.updateJob(job);
	}

	/**
	 * Wait for a job to complete by polling the Bulk API.
	 */
	public void awaitCompletion(BulkConnection connection, JobInfo job, List<BatchInfo> batchInfoList)
			throws AsyncApiException {
		long sleepTime = 0L;
		Set<String> incomplete = new HashSet<String>();
		for (BatchInfo bi : batchInfoList) {
			incomplete.add(bi.getId());
		}
		while (!incomplete.isEmpty()) {
			try {
				Thread.sleep(sleepTime);
			} catch (InterruptedException e) {
			}
			// System.out.println("We are uploading your file...Please wait..."
			// + incomplete.size());
			// System.out.println("We are uploading your file...Please
			// wait...");
			sleepTime = 10000L;
			BatchInfo[] statusList = connection.getBatchInfoList(job.getId()).getBatchInfo();
			for (BatchInfo b : statusList) {
				if (b.getState() == BatchStateEnum.Completed || b.getState() == BatchStateEnum.Failed) {
					if (incomplete.remove(b.getId())) {
						// System.out.println("BATCH STATUS:\n" + b);
					}
				}
			}
		}
	}

	/**
	 * Gets the results of the operation and checks for errors.
	 */
	public Boolean checkResults(BulkConnection connection, JobInfo job, List<BatchInfo> batchInfoList)
			throws AsyncApiException, IOException {
		// batchInfoList was populated when batches were created and submitted
		List<String> Ids = new ArrayList<String>();
		Boolean isSuccess = true;
		for (BatchInfo b : batchInfoList) {
			CSVReader rdr = new CSVReader(connection.getBatchResultStream(job.getId(), b.getId()));
			List<String> resultHeader = rdr.nextRecord();
			int resultCols = resultHeader.size();

			List<String> row;
			while ((row = rdr.nextRecord()) != null) {
				Map<String, String> resultInfo = new HashMap<String, String>();
				for (int i = 0; i < resultCols; i++) {
					resultInfo.put(resultHeader.get(i), row.get(i));
				}
				boolean success = Boolean.valueOf(resultInfo.get("Success"));
				boolean created = Boolean.valueOf(resultInfo.get("Created"));
				String id = resultInfo.get("Id");
				String error = resultInfo.get("Error");
				if (success && created) {
					// System.out.println("Created row with id " + id);
					Ids.add(id);
					isSuccess = true;
				} else if (!success) {
					isSuccess = false;
					System.out.println("Failed with error: " + error);
				}
			}
		}
		return isSuccess;
	}

}
