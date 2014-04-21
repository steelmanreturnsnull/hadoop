package hdfs;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.PrivilegedExceptionAction;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;

public class HDFSClient {

	private static FileSystem		fileSystem	= null;
	private static final String	user				= "pinjhuan";

	private static void ls(String path) throws FileNotFoundException, IllegalArgumentException, IOException {
		FileStatus[] fs = fileSystem.listStatus(new Path(path));

		for (FileStatus f : fs) {
			System.out.println(f.toString());
		}
	}

	private static void cat(String path) throws IllegalArgumentException, IOException {
		DataInputStream input = fileSystem.open(new Path(path));
		BufferedReader br = new BufferedReader(new InputStreamReader(input));
		String line = br.readLine();
		while (line != null) {
			System.out.println(line);
			line = br.readLine();
		}
		br.close();
		input.close();
	}

	private static void mkdir(String path) throws IllegalArgumentException, IOException {
		fileSystem.mkdirs(new Path(path));
		System.out.println("Successfully created path " + path + ".");
	}

	private static void chgrp(String path, String username, String groupname) throws FileNotFoundException, IllegalArgumentException, IOException {
		fileSystem.setOwner(new Path(path), username, groupname);
		System.out.println("Successfully changed path " + path + " owner to " + username + " group to " + groupname + ".");
	}

	private static void chmod(String path, String mode) throws FileNotFoundException, IllegalArgumentException, IOException {
		fileSystem.setPermission(new Path(path), new FsPermission(mode));
		System.out.println("Successfully changed path " + path + "permission to " + mode + ".");
	}

	private static void rm(String path) throws IllegalArgumentException, IOException {
		fileSystem.delete(new Path(path), true); // recursively
		System.out.println("Successfully deleted path " + path + ".");
	}

	private static void copyFromLocal(String fromPath, String toPath) throws IllegalArgumentException, IOException {
		fileSystem.copyFromLocalFile(new Path(fromPath), new Path(toPath));
		System.out.println("Successfully copied path " + fromPath + " to " + toPath + ".");
	}

	private static void copyToLocal(String fromPath, String toPath) throws IllegalArgumentException, IOException {
		fileSystem.copyToLocalFile(false, new Path(fromPath), new Path(toPath), true); // useRawLocalFileSystem = true, delSrc = false
		System.out.println("Successfully copied path " + fromPath + " to " + toPath + ".");
	}

	private static void printUsage(String command) {
		if (command.equals("ls"))
			System.out.println("Usage: ls <dir>");

		if (command.equals("cat"))
			System.out.println("Usage: cat <dir>");

		if (command.equals("chgrp"))
			System.out.println("Usage: chgrp <dir> <username> <groupname>");

		if (command.equals("mkdir"))
			System.out.println("Usage: mkdir <dir>");

		if (command.equals("rm"))
			System.out.println("Usage: rm <dir>");

		if (command.equals("copyFromLocal"))
			System.out.println("Usage: copyFromLocal <fromDir> <toDir>");

		if (command.equals("copyToLocal"))
			System.out.println("Usage: copyToLocal <fromDir> <toDir>");
		
		if (command.equals("chmod"))
			System.out.println("Usage: chmod <dir> <mode>");

		if (command.equals("help")) {
			System.out.println("Usage: ls <dir>");
			System.out.println("Usage: cat <dir>");
			System.out.println("Usage: chgrp <dir> <username> <groupname>");
			System.out.println("Usage: mkdir <dir>");
			System.out.println("Usage: rm <dir>");
			System.out.println("Usage: copyFromLocal <fromDir> <toDir>");
			System.out.println("Usage: copyToLocal <fromDir> <toDir>");
			System.out.println("Usage: chmod <dir> <mode>");
			System.out.println("Usage: exit");
		}
	}

	public static void main(String[] args) throws IOException, InterruptedException {

		UserGroupInformation ugi = UserGroupInformation.createRemoteUser(user);

		ugi.doAs(new PrivilegedExceptionAction<Void>() {

			@Override
			public Void run() throws Exception {

				Configuration conf = new Configuration();

				conf.addResource(new Path("src/conf/core-site.xml"));
				conf.addResource(new Path("src/conf/mapred-site.xml"));
				conf.reloadConfiguration();

				fileSystem = FileSystem.get(conf);

				BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
				while (true) {
					String line = br.readLine();
					Scanner scanner = new Scanner(line);

					String command = scanner.next();
					if (command.equals("ls")) {
						try {
							String dir = scanner.next();
							ls(dir);
						} catch (Exception e) {
							e.printStackTrace();
							printUsage(command);
						}
						continue;
					}

					if (command.equals("cat")) {
						try {
							String dir = scanner.next();
							cat(dir);
						} catch (Exception e) {
							e.printStackTrace();
							printUsage(command);
						}
						continue;
					}

					if (command.equals("mkdir")) {
						try {
							String dir = scanner.next();
							mkdir(dir);
						} catch (Exception e) {
							e.printStackTrace();
							printUsage(command);
						}
						continue;
					}

					if (command.equals("chgrp")) {
						try {
							String dir = scanner.next();
							String username = scanner.next();
							String group = scanner.next();
							chgrp(dir, username, group);
						} catch (Exception e) {
							e.printStackTrace();
							printUsage(command);
						}
						continue;
					}

					if (command.equals("rm")) {
						try {
							String dir = scanner.next();
							rm(dir);
						} catch (Exception e) {
							e.printStackTrace();
							printUsage(command);
						}
						continue;
					}

					if (command.equals("copyFromLocal")) {
						try {
							String fromDir = scanner.next();
							String toDir = scanner.next();
							copyFromLocal(fromDir, toDir);
						} catch (Exception e) {
							e.printStackTrace();
							printUsage(command);
						}
						continue;
					}

					if (command.equals("copyToLocal")) {
						try {
							String fromDir = scanner.next();
							String toDir = scanner.next();
							copyToLocal(fromDir, toDir);
						} catch (Exception e) {
							e.printStackTrace();
							printUsage(command);
						}
						continue;
					}
					
					if (command.equals("chmod")) {
						try {
							String dir = scanner.next();
							String mode = scanner.next();
							chmod(dir, mode);
						} catch (Exception e) {
							e.printStackTrace();
							printUsage(command);
						}
						continue;
					}

					if (command.equals("exit")) {
						scanner.close();
						System.exit(0);
					}

					if (command.equals("help")) {
						printUsage("help");
						continue;
					}

					System.out.println("Invalid command");
					printUsage("help");

				}

			}
		});
	}
}
