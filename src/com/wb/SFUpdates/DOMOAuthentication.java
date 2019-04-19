/**
 * @author Srinizkumar Konakanchi
 *
 */

package com.wb.SFUpdates;

import com.domo.sdk.DomoClient;
import com.domo.sdk.request.Config;
import com.domo.sdk.request.Scope;

import okhttp3.logging.HttpLoggingInterceptor;

public class DOMOAuthentication {
	
	public static DomoClient authenticateToDomo(){
		
		//Build an SDK configuration
        Config config = Config.with()
                .clientId("xxxxxx")
                .clientSecret("xxxx")
                .apiHost("api.domo.com")
                .useHttps(true)
                .scope(Scope.DATA,Scope.USER)
                .httpLoggingLevel(HttpLoggingInterceptor.Level.BODY)
                .build();

        //Create an instance of the SDK Client
        DomoClient domo = DomoClient.create(config);
        
        return domo;
	}

}
