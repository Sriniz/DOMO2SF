/**
 * @author Srinizkumar Konakanchi
 *
 */

package com.wb.SFUpdates;

/**
 *
 */
import com.sforce.async.*;
import com.sforce.soap.partner.*;
import com.sforce.ws.ConnectorConfig;

public class ConnectionInformation {

	PartnerConnection pConn;
	BulkConnection rConn;
	ConnectorConfig partnerConfig;

	public ConnectionInformation(PartnerConnection pConn, BulkConnection rConn, ConnectorConfig partnerConfig) {
		this.pConn = pConn;
		this.rConn = rConn;
		this.partnerConfig = partnerConfig;
	}

}
