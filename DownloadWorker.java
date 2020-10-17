

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;

public class DownloadWorker implements Runnable {
	private final static int BUFFER_SIZE = 256 * 1024;
	private final static int WAIT_ON_ERROR_MILLISECONDS = 2000;
	private final static int CONNECTION_READ_TIMEOUT = 1000;
	private final static int CONNECTION_TIMEOUT = 2000;

	private int id;
	private String[] urls;

	private List<Segment> segments;
	private LinkedBlockingQueue<SegmentPayload> queue;

	/**
	 * @param id       - worker id
	 * @param urls     - urls to download from
	 * @param segments - segments to download
	 * @param queue    - LinkedBlockingQueue to put the results in for disk writer
	 */
	public DownloadWorker(int id, String[] urls, List<Segment> segments, LinkedBlockingQueue<SegmentPayload> queue) {
		this.id = id;
		this.urls = urls;

		this.segments = segments;

		this.queue = queue;
	}

	public void run() {

		Segment segment;
		while ((segment = Segment.GetOne(this.segments)) != null) {
			String url = this.getUrl();
			System.out.printf("[%d] Start downloading range (%d - %d) from %s\n",
					this.id, segment.getStartIndex(), segment.getEndIndex(), url);

			segment.setState(Segment.SegmentState.IN_PROGRESS);

			this.downloadSegment(segment, url);
		}
	}

	/**
	 * @return one of the urls randomly
	 */
	public String getUrl() {
		int rnd = new Random().nextInt(this.urls.length);
		return this.urls[rnd];
	}


	/**
	 * Downloads a segment and send it to the queue
	 *
	 * @param segment - the segment to download
	 */
	private void downloadSegment(Segment segment, String url) {
		HttpURLConnection conn;

		while (true) {
			long startIndex = segment.getStartIndex();
			long endIndex = segment.getEndIndex();

			try {

				URL link = new URL(url);
				conn = (HttpURLConnection) link.openConnection();
				conn.setRequestProperty("Range", "bytes=" + startIndex
						+ "-" + endIndex);
				conn.setReadTimeout(CONNECTION_READ_TIMEOUT);
				conn.setConnectTimeout(CONNECTION_TIMEOUT);
				int responseCode = conn.getResponseCode();
				if (responseCode != HttpURLConnection.HTTP_OK &&
						responseCode != HttpURLConnection.HTTP_PARTIAL) {
					throw new IOException("Invalid response code received: " + responseCode);
				}

			} catch (MalformedURLException e) {

				System.err.println("URL is invalid: " + e.getMessage());
				return;
			} catch (IOException e) {

				System.err.println("Can't open connection: " + e.getMessage());
				sleep(WAIT_ON_ERROR_MILLISECONDS);
				return;
			}

			try {
				boolean finished = this.produceStream(segment, startIndex, endIndex, conn.getInputStream());
				if (finished) {
					segment.setState(Segment.SegmentState.FINISHED_PRODUCING);
				}

				return;

			} catch (IOException e) {
				System.err.println("Error while get input stream: " + e.getMessage());
			} catch (Exception e) {
				System.err.println("Error while downloading segment: " + e.getMessage());
			}
		}
	}

	/**
	 * @param segment - the segment that the stream relates to
	 * @param stream  - the stream to download and send to the queue
	 * @return
	 */
	private boolean produceStream(Segment segment, long startIndex, long endIndex, InputStream stream) {
		int len;
		byte[] buffer = new byte[BUFFER_SIZE];

		long seek = startIndex;

		long actualDownload = 0L;
		long expectedDownload = endIndex - startIndex;

		try {
			// read will return -1 when done
			while ((len = stream.read(buffer)) != -1) {
				SegmentPayload segmentPayload = new SegmentPayload(segment, Arrays.copyOf(buffer, len), len, seek);
				this.queue.put(segmentPayload);

				seek += len;
				actualDownload += len;
			}

		} catch (IOException e) {
			System.err.println("Error while reading from stream: " + e.getMessage());
			return false;
		} catch (InterruptedException e) {
			System.err.println("Interrupted while waiting to put in queue: " + e.getMessage());
			return false;
		}

		if (actualDownload < expectedDownload) {
			return false;
		}

		return true;
	}

	/**
	 * @param milliseconds - number of milliseconds to sleep
	 * @return true if sleep was successful
	 */
	private boolean sleep(int milliseconds) {
		try {

			Thread.sleep(milliseconds);
		} catch (InterruptedException e) {
			return false;
		}

		return true;
	}


}
