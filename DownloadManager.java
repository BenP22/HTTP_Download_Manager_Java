


import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class DownloadManager {
	private final static int POLL_TIMEOUT_SECONDS = 1;
	private final static int DOWNLOAD_PERCENTAGE_INTERVAL_MILLISECONDS = 500;
	private static final int QUEUE_CAPACITY = 16 * 1024 * 1024;
	private static final int MINIMUM_FILE_SIZE_TO_USE_THREADS = 1024 * 1024;
	private static final int THREADS_TO_USE_ON_SMALL_FILE = 1;
	private final static long SEGMENT_SIZE = 4 * 1024;

	private DownloadMetadata metadata;
	private DownloadWorker[] downloadWorkers;

	/**
	 * @param urls         list of urls to download from
	 * @param numOfWorkers number of workers to use (threads)
	 * @throws Exception
	 */
	DownloadManager(String[] urls, int numOfWorkers) throws Exception {

		metadata = new DownloadMetadata(urls);

		List<Segment> segments = metadata.getSegments();

		if (metadata.getFileSize() < MINIMUM_FILE_SIZE_TO_USE_THREADS) {
			System.out.println("File is smaller than 1M, using one thread");
			numOfWorkers = THREADS_TO_USE_ON_SMALL_FILE;
		}

		if (segments == null) {
			segments = Segment.GetSegments(0, metadata.getFileSize(), SEGMENT_SIZE);
		}

		List<List<Segment>> calculatedSegments = GetCalculatedSegments(segments, numOfWorkers);

		metadata.setSegments(calculatedSegments);

		LinkedBlockingQueue<SegmentPayload> queue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);

		downloadWorkers = new DownloadWorker[calculatedSegments.size()];
		Thread[] downloadWorkerThreads = new Thread[calculatedSegments.size()];

		for (int i = 0; i < calculatedSegments.size(); i++) {
			downloadWorkers[i] = new DownloadWorker(i, this.metadata.getUrls(), calculatedSegments.get(i), queue);
			downloadWorkerThreads[i] = new Thread(downloadWorkers[i]);

			downloadWorkerThreads[i].start();
		}

		// Start percentage & metadata thread
		DownloadPercentage downloadPercentage = new DownloadPercentage();
		Thread downloadPercentageThread = new Thread(downloadPercentage);
		downloadPercentageThread.start();

		try (RandomAccessFile file = new RandomAccessFile(metadata.getFileName(), "rwd")) {
			while (true) {
				SegmentPayload segmentPayload;

				segmentPayload = queue.poll(POLL_TIMEOUT_SECONDS, TimeUnit.SECONDS);
				if (segmentPayload == null) {
					if (this.WorkersFinishedProducing()) {
						break;
					}
					continue;
				}

				// Get needed data before writing
				long seek = segmentPayload.getSeek();
				int length = segmentPayload.getLength();
				byte[] data = segmentPayload.getData();

				try {
					file.seek(seek);
					file.write(data, 0, length); // Use 0 offset to write for the beginning of the buffer

					// Update new start after writing bytes
					segmentPayload.getSegment().setStartIndex(seek + length);

					this.metadata.addBytesRead(length);

					// serialize after every write to disk
					this.metadata.serialize();

				} catch (IOException e) {
					System.err.println("Error on file writer: " + e.getMessage());
				}
			}
		}

		for (int i = 0; i < downloadWorkerThreads.length; i++) {
			downloadWorkerThreads[i].join();
		}
		downloadPercentage.Stop();

		if (this.metadata.isCompleted()) {
			this.metadata.Clean();
			System.out.println("Download succeeded");
		} else {
			System.err.println("Download failed");
		}
	}

	/**
	 * @return true if all workers have finished producing segments
	 */
	private boolean WorkersFinishedProducing() {
		for (Segment segment : this.metadata.getSegments()) {
			if (segment.getState() != Segment.SegmentState.FINISHED_PRODUCING) {
				return false;
			}
		}

		return true;
	}

	/**
	 * @param segmentsInput - list of segments to partition and compress
	 * @param numOfWorkers  - number of workers to split for
	 * @return
	 */
	private List<List<Segment>> GetCalculatedSegments(List<Segment> segmentsInput, int numOfWorkers) {
		List<Segment> segments = new ArrayList<>();
		for (Segment segment : segmentsInput) {
			if (segment.getStartIndex() < segment.getEndIndex()) {
				segment.setState(Segment.SegmentState.AVAILABLE);
				segments.add(segment);
			}
		}

		if (segments.size() < numOfWorkers) {
			segments = Segment.InflateSegments(metadata.getSegments(), numOfWorkers);
		}

		List<List<Segment>> partitionSegments = Segment.PartitionSegments(segments, numOfWorkers);

		List<List<Segment>> compressedSegments = new ArrayList<>();

		for (List<Segment> segmentList : partitionSegments) {
			compressedSegments.add(Segment.CompressSegments(segmentList));
		}

		return compressedSegments;
	}


	/**
	 * Show the percentage of the download and save the metadata on every interval
	 */
	private class DownloadPercentage implements Runnable {

		private boolean stopRunning = false;
		private int lastReportedPercentage = -1; // unique initial value

		public synchronized void Stop() {
			this.stopRunning = true;
		}

		private synchronized boolean keepRunning() {
			return !this.stopRunning;
		}

		@Override
		public void run() {
			while (keepRunning()) {
				int newPercentage = getDownloadPercentage();
				if (newPercentage != lastReportedPercentage) {
					System.out.printf("Downloaded %d%%\n", newPercentage);
				}

				lastReportedPercentage = newPercentage;

				try {
					Thread.sleep(DOWNLOAD_PERCENTAGE_INTERVAL_MILLISECONDS);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * @return the percentage that was downloaded until now
	 */
	private int getDownloadPercentage() {
		long completed = this.metadata.getBytesRead();

		// Calculate the percentage given completed and file size.
		return (int) (0.5d + ((double) completed / (double) this.metadata.getFileSize()) * 100);
	}


}
