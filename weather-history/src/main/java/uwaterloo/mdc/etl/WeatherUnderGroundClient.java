package uwaterloo.mdc.etl;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Calendar;
import java.util.GregorianCalendar;

import org.apache.commons.io.FileUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;

public class WeatherUnderGroundClient {
	
	private final static String wuUrlStr = "http://www.wunderground.com/history/airport/LSGG/%d/%d/%d/DailyHistory.html?req_city=NA&req_state=NA&req_statename=NA&theprefset=SHOWMETAR&theprefvalue=1&format=1";
	private static final long END_UNIX_TIME = (new GregorianCalendar(2011,Calendar.SEPTEMBER,1).getTimeInMillis() / 1000) - 1;
	public final static void main(String[] args) throws Exception {
		String outDir = "D:\\datasets\\weather-underground";
        HttpClient httpclient = new DefaultHttpClient();
        try {
        	Calendar calendar = new GregorianCalendar(2009,Calendar.SEPTEMBER,1);
        	long dayEndUxTime = -1;
        	long dayStartUxTime = -1;
        	do {
        		
            	String url = String.format(wuUrlStr, calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH) + 1, calendar.get(Calendar.DAY_OF_MONTH));
        		
            HttpGet httpget = new HttpGet(url);

            
            // Execute HTTP request
            System.out.println("executing request " + httpget.getURI());
            HttpResponse response = httpclient.execute(httpget);

            System.out.println("----------------------------------------");
            System.out.println(response.getStatusLine());
            System.out.println("----------------------------------------");

            // Get hold of the response entity
            HttpEntity entity = response.getEntity();

            // If the response does not enclose an entity, there is no need
            // to bother about connection release
            if (entity != null) {
            	
                InputStream instream = entity.getContent();
                try {
                	dayStartUxTime = calendar.getTimeInMillis() / 1000;
                	calendar.add(Calendar.DAY_OF_MONTH,1);
                	dayEndUxTime = (calendar.getTimeInMillis() / 1000) - 1;
                	
                	String filename = "LSGG_"+dayStartUxTime+"-"+dayEndUxTime+".csv";
                	File fileOut = new File(outDir, filename);
                	
                	FileUtils.copyInputStreamToFile(instream, fileOut);
                	
                } catch (IOException ex) {
                    // In case of an IOException the connection will be released
                    // back to the connection manager automatically
                    ex.printStackTrace();
                } catch (RuntimeException ex) {
                    // In case of an unexpected exception you may want to abort
                    // the HTTP request in order to shut down the underlying
                    // connection immediately.
                    httpget.abort();
                    ex.printStackTrace();
                } finally {
                    // Closing the input stream will trigger connection release
                    try { instream.close(); } catch (Exception ignore) {}
                }
            }
            
        	} while(dayEndUxTime < END_UNIX_TIME);

        } finally {
            // When HttpClient instance is no longer needed,
            // shut down the connection manager to ensure
            // immediate deallocation of all system resources
            httpclient.getConnectionManager().shutdown();
        }
    }
}
