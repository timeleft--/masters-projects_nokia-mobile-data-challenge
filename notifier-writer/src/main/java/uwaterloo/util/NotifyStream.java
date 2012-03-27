package uwaterloo.util;

import java.io.ByteArrayOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.io.FileUtils;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthState;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.ClientContext;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.protocol.HttpContext;

public class NotifyStream extends FilterOutputStream {

	class SendCopyCallable implements Runnable {
		private SimpleDateFormat timestampFmt = new SimpleDateFormat(
				"_yyyy-MM-dd_HH-mm-ss");
		private static final String UPLOAD_BASE_URL = "C:\\Users\\yaboulna\\Dropbox\\notify-writer-logs\\";
		// "http://plg.uwaterloo.ca/~yaboulna/watapp/research/aplog";
		// //yaboulna/notify not workin
		private static final long INTERVAL = 5 * 1000; // *60;

		public void run() {
			while (true) {
				try {
					send();
					try {
						Thread.sleep(INTERVAL);
					} catch (InterruptedException expected) {
						send();
						break;
					}

				} catch (Exception ignored) {
					// naaah
				}
			}
			return;
		}

		void send() throws ClientProtocolException, IOException {
			byte[] pendingBytes;
			synchronized (copyOut) {

				pendingBytes = copyOut.toByteArray();

				copyOut.reset();
			}
			if (pendingBytes.length > 0) {

				if (UPLOAD_BASE_URL.startsWith("http")) {
					DefaultHttpClient httpClient = new DefaultHttpClient();
					httpClient.addRequestInterceptor(
							new HttpRequestInterceptor() {
								public void process(HttpRequest request,
										HttpContext context)
										throws HttpException, IOException {
									AuthState authState = (AuthState) context
											.getAttribute(ClientContext.TARGET_AUTH_STATE);
									authState.setAuthScheme(new BasicScheme());
									authState
											.setCredentials(new UsernamePasswordCredentials(
													"watappput:70tt0WatApp"));
								}
							}, 0);

					ByteArrayEntity logDataByteArrayEntity = new ByteArrayEntity(
							pendingBytes);
					HttpPost post = new HttpPost(UPLOAD_BASE_URL
							+ "/logupload.py");
					post.addHeader("User-Agent", processName);
					post.setEntity(logDataByteArrayEntity);

					HttpResponse response = httpClient.execute(post);
					int responseCode = response.getStatusLine().getStatusCode();
					responseCode -= 200;
					if (responseCode < 0 || responseCode >= 100) {
						synchronized (copyOut) {
							copyOut.write(pendingBytes);
						}
					}

				} else {
					FileUtils
							.writeByteArrayToFile(FileUtils.getFile(
									UPLOAD_BASE_URL,
									processName
											+ timestampFmt.format(new Date())
											+ ".log"), pendingBytes, true);
				}
			}
		}
	}

	private final ByteArrayOutputStream copyOut;
	final String processName;
	final Thread notifyThread;

	public NotifyStream(OutputStream out, String processName) {
		super(out);
		this.processName = processName;
		copyOut = new ByteArrayOutputStream();
		notifyThread = new Thread(new SendCopyCallable());
		notifyThread.start();
	}

	@Override
	public void write(byte[] cbuf, int off, int len) throws IOException {
		synchronized (copyOut) {
			copyOut.write(cbuf, off, len);
		}
		super.write(cbuf, off, len);

	}

	@Override
	public void flush() throws IOException {
		synchronized (copyOut) {
			copyOut.flush();
		}
		super.flush();
	}

	@Override
	public void close() throws IOException {
		notifyThread.interrupt();
		while (notifyThread.isAlive()) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException ignored) {
				// nah
			}
		}
		super.close();
	}

}
