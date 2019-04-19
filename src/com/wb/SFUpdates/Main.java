/**
 * @author Srinizkumar Konakanchi
 *
 */

package com.wb.SFUpdates;

import java.io.File;
import java.io.IOException;

import com.domo.sdk.DomoClient;
import com.domo.sdk.datasets.DataSetClient;
import com.sforce.async.AsyncApiException;
import com.sforce.async.BulkConnection;
import com.sforce.async.JobInfo;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;

public class Main {
	
	public static void main(String[] args) {
		updateSFObjects();
	}
	
	public static void updateSFObjects() {
        
        // Create an instance of the SDK Client
        DomoClient domoConnection = DOMOAuthentication.authenticateToDomo();
        
        //Salesforce connection
        ConnectionPartner pc = new ConnectionPartner();
    	ConnectionInformation connInfo;
    	BulkConnection salesforceConnection = null;
		try {
			connInfo = pc.getRestConnection();
			salesforceConnection = connInfo.rConn;
		} catch (ConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (AsyncApiException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
		/*
        //Load MPM EIDR updates into Salesforce
    	uploadToSalesforce(domoConnection, salesforceConnection, "42bd055b-081d-4380-bf0c-a8d0d9636690", "MPM_EIDR_Updates.csv", "MPM__c","Update");
        
    	//Load Title EIDR updates into Salesforce
    	uploadToSalesforce(domoConnection, salesforceConnection, "c67c284e-4575-486f-8313-d762f653df8e", "Title_EIDR_Updates.csv", "Title__c","Update");
    	
    	//Load Title ALID updates into Salesforce
    	uploadToSalesforce(domoConnection, salesforceConnection, "bdc34d67-b14a-4588-973f-71f967789f7f", "Title_ALID_Updates.csv", "Title__c","Update");
        
    	//Load Local Data UV updates into Salesforce
    	uploadToSalesforce(domoConnection, salesforceConnection, "54f8ae59-9317-4aa0-950e-60c7a18b447c", "LD_UV_Updates.csv", "Local_Title__c","Update");
        
    	//Load Local Data Edit EIDR updates into Salesforce
    	uploadToSalesforce(domoConnection, salesforceConnection, "307245f5-4181-4dc5-9dc9-5b5ed31a77d0", "LD_EditEIDR_Updates.csv", "Local_Title__c","Update");
    	
    	//Load Local Data 2D EIDR updates into Salesforce
    	uploadToSalesforce(domoConnection, salesforceConnection, "c6c5c6e0-388f-4cc7-9203-033089a8236a", "LD_2DEIDR_Updates.csv", "Local_Title__c","Update");
        
    	//Load Local Data 3D EIDR updates into Salesforce
    	uploadToSalesforce(domoConnection, salesforceConnection, "5e3876fe-e69c-4495-b3df-7be855cee299", "LD_3DEIDR_Updates.csv", "Local_Title__c","Update");

    	//Load Client Avail RPID and VID updates into Salesforce
    	uploadToSalesforce(domoConnection, salesforceConnection, "838b613f-e9cd-4638-b279-d77b2069f7c1", "CA_RPID_VID_Updates.csv", "Client_Avail__c","Update");
    	
    	//National Pricebook Changes For Upload
    	uploadToSalesforce(domoConnection, salesforceConnection, "0c0cdaee-5472-46bd-9b0c-b3141f6da558", "National_Pricebook_Changes.csv", "Pricebook_Entry_Period__c","Update");

    	//National Pricebook Deletes (but actually an update) For Upload
    	uploadToSalesforce(domoConnection, salesforceConnection, "a187e6e3-00c2-4637-85cc-b2b9b2ac8a1f", "National_Pricebook_Changes_1.csv", "Pricebook_Entry_Period__c","Update");
		*/
    	//National Pricebook Additions For Upload
    	uploadToSalesforce(domoConnection, salesforceConnection, "c5ba2a39-3b3d-4af2-a61a-71f1795a9c4f", "National_Pricebook_Additions.csv", "Pricebook_Entry_Period__c","Insert");

    }
	
	public static void uploadToSalesforce(DomoClient domoConnection, BulkConnection salesforceConnection, String domoFileID, String csvTempFileName, String sfObject, String operationType){
		
		File f = new File(csvTempFileName);
        try {
			f.createNewFile();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        DataSetClient datasets = domoConnection.dataSetClient();
        datasets.get(domoFileID);
        datasets.exportDataToFile( domoFileID, true, f);
        System.out.println(csvTempFileName + " update file has been extracted from DOMO...");

        try{
        	CreateBulkLoadJobs cbl = new CreateBulkLoadJobs();
        	JobInfo job = new JobInfo();
        	if(operationType.equals("Update"))
        		job = cbl.createUpdateJob(sfObject, salesforceConnection);
        	if(operationType.equals("Insert"))
        		job = cbl.createInsertJob(sfObject, salesforceConnection);
        	if(operationType.equals("Delete"))
        		job = cbl.createDeleteJob(sfObject, salesforceConnection);
        	BatchInformation batchInfo = cbl.createBatchesFromCSVFile(salesforceConnection, job, csvTempFileName);
        	cbl.closeJob(salesforceConnection, job.getId());
        	cbl.awaitCompletion(salesforceConnection, job, batchInfo.batchInfoList);
        	cbl.checkResults(salesforceConnection, job, batchInfo.batchInfoList);
        	System.out.println(csvTempFileName + " update file has been loaded into salesforce...");
        } catch(Exception e){
        	e.printStackTrace();
        }

	}
	
}
