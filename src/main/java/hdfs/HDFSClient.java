package hdfs;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSClient {

	private static FileSystem	fileSystem	= null;

	private static void ls(String path) throws FileNotFoundException, IllegalArgumentException, IOException {
		FileStatus[] fs = fileSystem.listStatus(new Path(path));

		for (FileStatus f : fs) {
			System.out.println(f.toString());
		}
	}

	private static void cat(String path) throws FileNotFoundException, IllegalArgumentException, IOException {
		DataInputStream input = fileSystem.open(new Path(path));
		BufferedReader br = new BufferedReader(new InputStreamReader(input));
		String line = br.readLine();
		while (line != null) {
			System.out.println(line);
			line = br.readLine();
		}
	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();

		conf.set("fs.defaultFS", "hdfs://buse01cn01.us.oracle.com:8020");

		fileSystem = FileSystem.get(conf);

		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		while (true) {
			String line = br.readLine();
			Scanner scanner = new Scanner(line);

			String command = scanner.next();
			if (command.equals("ls")) {
				String dir = scanner.next();
				ls(dir);
				continue;
			}

			if (command.equals("cat")) {
				String dir = scanner.next();
				cat(dir);
				continue;
			}

			if (command.equals("exit")) {
				System.exit(0);
			}

			System.out.println("Invalid command");

		}

	}

}
