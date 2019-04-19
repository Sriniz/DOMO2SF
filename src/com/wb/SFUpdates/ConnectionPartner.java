/**
 * @author Srinizkumar Konakanchi
 *
 */

package com.wb.SFUpdates;

import com.sforce.async.AsyncApiException;
import com.sforce.async.BulkConnection;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;

public class ConnectionPartner {

	/**
	 * Create the RestConnection used to call Bulk API operations.
	 */
	public ConnectionInformation getRestConnection()
			throws ConnectionException, AsyncApiException {
		
		/*
		// Salesforce.com credentials
     	String userName = "wbhesalesservices@warnerbros.com";
     	String password = "abcd#1234";
     	String endPoint = "https://login.salesforce.com/services/Soap/u/37.0";
		*/
		
		// Salesforce.com credentials
     	String userName = "xxxxxx";
     	String password = "xxxxxx";
     	String endPoint = "https://test.salesforce.com/services/Soap/u/37.0";
     	
		PartnerConnection pConn;
		ConnectorConfig partnerConfig = new ConnectorConfig();
		partnerConfig.setUsername(userName);
		partnerConfig.setPassword(password);
		partnerConfig.setAuthEndpoint(endPoint);
		// Creating the connection automatically handles login and stores
		// the session in partnerConfig
		// eConnection = Connector.newConnection(partnerConfig);
		pConn = new PartnerConnection(partnerConfig);
		// code for r&d

		// When PartnerConnection is instantiated, a login is implicitly
		// executed and, if successful,
		// a valid session is stored in the ConnectorConfig instance.
		// Use this key to initialize a RestConnection:
		ConnectorConfig config = new ConnectorConfig();
		config.setSessionId(partnerConfig.getSessionId());
		// The endpoint for the Bulk API service is the same as for the normal
		// SOAP uri until the /Soap/ part. From here it's '/async/versionNumber'
		String soapEndpoint = partnerConfig.getServiceEndpoint();
		// System.out.println("soapEndpoint"+soapEndpoint);
		String apiVersion = "37.0";
		String restEndpoint = soapEndpoint.substring(0, soapEndpoint.indexOf("Soap/")) + "async/" + apiVersion;
		config.setRestEndpoint(restEndpoint);
		// System.out.println("restEndpoint.."+restEndpoint);
		// This should only be false when doing debugging.
		config.setCompression(true);
		// Set this to true to see HTTP requests and responses on stdout
		config.setTraceMessage(false);
		BulkConnection connection = new BulkConnection(config);
		return new ConnectionInformation(pConn, connection, partnerConfig);
	}

}
