import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;

/**
 * Metadata of the download
 */
public class DownloadMetadata implements Serializable {
	private static final long serialVersionUID = 1234892138585L;
	private static final int SLEEP_ON_SERIALIZATION_FAIL = 1000; // in milliseconds
	private static final int RETRIES_ON_SERIALIZATION_FAIL = 1000;

	private final static String METADATA_FOLDER_PATH = "./";

	private long fileSize;
	private long bytesRead;

	private String[] urls;
	private String fileName;
	private String metadataPath;

	private List<Segment> segments;

	private boolean rangeEnabled;

	/**
	 * @param urls - list of urls to use in the download
	 * @throws IOException
	 */
	DownloadMetadata(String[] urls) throws IOException {
		if (urls.length < 1) {
			throw new IllegalArgumentException("Urls list must have at least one url");
		}

		this.urls = urls;
		this.fileSize = 0L; // default value before downloading something
		this.bytesRead = 0L; // default value before downloading something
		this.rangeEnabled = false;

		// Take the filename from the url itself
		fileName = urls[0].substring(urls[0].lastIndexOf("/") + 1);

		metadataPath = METADATA_FOLDER_PATH + this.fileName + ".metadata";

		downloadMetadata();
	}

	/**
	 * Delete all metadata files that were created
	 */
	public void Clean() {
		File metadataFile = new File(this.metadataPath);

		if (metadataFile.exists()) {
			boolean delete = metadataFile.delete();
			if (!delete) {
				System.err.println("Something went wrong while deleting the metadata file");
			}
		}

		File metadataTempFile = new File(this.metadataPath + ".tmp");

		if (metadataTempFile.exists()) {
			boolean delete = metadataTempFile.delete();
			if (!delete) {
				System.err.println("Something went wrong while deleting the metadata temp file");
			}
		}
	}

	/**
	 * Download the metadata from the server (file size, if range is supported)
	 *
	 * @throws IOException
	 */
	private void downloadMetadata() throws IOException {
		// if we already have downloaded metadata, skip this part
		if (deserialize()) {
			System.out.println("Using download metadata cache on disk");
			return;
		}
		URL link = new URL(this.urls[0]);

		HttpURLConnection urlConn = (HttpURLConnection) link.openConnection();

		int responseCode = urlConn.getResponseCode();
		if (responseCode != HttpURLConnection.HTTP_OK) {
			throw new IOException("Bad response code from server != 200 OK");
		}

		fileSize = urlConn.getContentLengthLong();

		// Check if server accepts ranges (all should accept in this exercise)
		String range = urlConn.getHeaderField("Accept-Ranges");
		if (range == null || range.isEmpty()) {
			rangeEnabled = false;
		} else {
			rangeEnabled = true;
		}

		serialize();
	}

	/**
	 * serialize the metadata to disk
	 */
	void serialize() {
		Exception err = new Exception();

		for (int i = 0; i < RETRIES_ON_SERIALIZATION_FAIL; i++) {
			try {
				FileOutputStream fileOut =
						new FileOutputStream(this.metadataPath + ".tmp");
				ObjectOutputStream out = new ObjectOutputStream(fileOut);
				out.writeObject(this);
				out.close();
				fileOut.close();

				Files.move(Paths.get(this.metadataPath + ".tmp"),
						Paths.get(this.metadataPath), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);

				return;
			} catch (Exception e) {
				err = new Exception("Failed on serialization: " + e.getMessage());

				try {
					Thread.sleep(SLEEP_ON_SERIALIZATION_FAIL);
				} catch (InterruptedException e1) {
					System.err.println("Failed to sleep on serialization retry");
				}
			}
		}

		System.err.printf("Failed on serialization after %d retries: %s", RETRIES_ON_SERIALIZATION_FAIL, err);
	}

	/**
	 * @return deserialize metadata from disk
	 */
	boolean deserialize() {
		DownloadMetadata downloadMetadata = null;
		try {
			FileInputStream fileIn = new FileInputStream(this.metadataPath);
			ObjectInputStream in = new ObjectInputStream(fileIn);
			downloadMetadata = (DownloadMetadata) in.readObject();

			this.fileName = downloadMetadata.fileName;
			this.fileSize = downloadMetadata.fileSize;
			this.bytesRead = downloadMetadata.bytesRead;
			this.urls = downloadMetadata.urls;
			this.rangeEnabled = downloadMetadata.rangeEnabled;
			this.segments = downloadMetadata.segments;

			in.close();
			fileIn.close();
		} catch (Exception ex) {
			return false;
		}

		return true;
	}

	public String[] getUrls() {
		return urls;
	}

	public long getFileSize() {
		return fileSize;
	}

	public String getFileName() {
		return fileName;
	}

	public List<Segment> getSegments() {
		return segments;
	}

	/**
	 * @param segmentsListOfLists - list of lists of segments(each list was given to a worker)
	 *                            in this class, we save them in a flat manner as the structure
	 *                            is irrelevant
	 */
	public void setSegments(List<List<Segment>> segmentsListOfLists) {
		ArrayList<Segment> segments = new ArrayList<>();

		for (List<Segment> segmentsList : segmentsListOfLists) {
			segments.addAll(segmentsList);
		}

		this.segments = segments;
		this.serialize();
	}

	public void addBytesRead(long numOfBytes) {
		this.bytesRead += numOfBytes;
		if (this.bytesRead >= this.fileSize) {
			this.bytesRead = this.fileSize;
		}
	}

	public long getBytesRead() {
		return bytesRead;
	}

	public boolean isCompleted() {
		return this.bytesRead >= this.fileSize;
	}
}
