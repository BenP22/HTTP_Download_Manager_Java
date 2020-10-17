

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class IdcDm {

	public static void main(String[] args) {

		if (args.length != 1 && args.length != 2) {
			System.err.println("Usage: \n\tjava IdcDm URL|URL-LIST-FILE [MAX-CONCURRENT-CONNECTIONS]");
			return;
		}

		int numOfWorkers = 1; // default number of workers

		if (args.length == 2) { // we expect a maximum of 2 arguments
			try {
				numOfWorkers = Integer.parseInt(args[1]);
			} catch (NumberFormatException e) {
				System.err.printf("Invalid number of workers: %s", e.getMessage());
				return;
			}
		}

		// args[0] is the url or file
		boolean isUrl;
		boolean isFile = false;

		try {
			new URL(args[0]);
			isUrl = true;

		} catch (MalformedURLException e) {
			isUrl = false;
		}

		if (!isUrl) {
			File f = new File(args[0]);

			isFile = f.exists();
		}

		if (!isUrl && !isFile) {
			System.err.println("First argument is not a url and not an existing file");
			return;
		}

		try {

			if (isUrl) {

				new DownloadManager(new String[]{args[0]}, numOfWorkers);

			} else {

				// args[0] is a file
				BufferedReader abc = new BufferedReader(new FileReader(args[0]));
				List<String> lines = new ArrayList<>();

				String line;
				while ((line = abc.readLine()) != null) {
					lines.add(line);
				}
				abc.close();

				String[] data = lines.toArray(new String[]{});

				new DownloadManager(data, numOfWorkers);
			}
		} catch (Exception e) {
			System.err.println("Error while downloading: " + e.getMessage());
		}
	}
}
