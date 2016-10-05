package cdstreams;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class FunWithStreams {

	public final List<File> FILES;

	public FunWithStreams() {
		FILES = Arrays.asList(new File("src/main/resources").listFiles());
	}

	public List<File> getDirs() {
		List<File> dirs = new ArrayList<>();
		for (File file : FILES) {
			if (file.isDirectory()) {
				dirs.add(file);
			}
		}
		return dirs;
	}
	

	public List<File> getNonEmptyDirs() {
		List<File> dirs = new ArrayList<>();
		for (File file : FILES) {
			if (file.isDirectory()) {
				for(File dirFile : file.listFiles()) {
					if(!dirFile.getName().equals("NEEDED_FOR_GIT")) {
						dirs.add(file);
					}
				}
			}
		}
		return dirs;
	}	

	public Optional<File> findFile(String filename) {
		for (File file : FILES) {
			if (file.getName().equals(filename)) {
				return Optional.of(file);
			}
		}
		return Optional.empty();
	}

	public boolean containsDirs() {
		for (File file : FILES) {
			if (file.isDirectory()) {
				return true;
			}
		}
		return false;
	}

	public boolean containsOnlyFiles() {
		for (File file : FILES) {
			if (file.isDirectory()) {
				return false;
			}
		}
		return true;
	}

	public List<String> getFilenames() {
		List<String> filenames = new ArrayList<>();
		for (File file : FILES) {
			filenames.add(file.getName());
		}
		return filenames;
	}

	public long getSumFilesize() {
		long filesize = 0;
		for (File file : FILES) {
			if (!file.isDirectory()) {
				filesize += file.length();
			}
		}
		return filesize;
	}

	public Map<String, Long> countFirstnames() {
		try {
			Map<String, Long> firstnameCount = new HashMap<>();
			for (File file : FILES) {
				if (file.getName().endsWith(".csv")) {
					List<String> lines = Files.readAllLines(file.toPath());
					for (String line : lines) {
						Row row = new Row(line);
						Long count = firstnameCount.get(row.getFirstname());
						if (count == null) {
							count = 0L;
						}
						firstnameCount.put(row.getFirstname(), ++count);
					}
				}
			}
			return firstnameCount;
		} catch (IOException ex) {
			throw new RuntimeException(ex);
		}
	}

	public Set<String> hashRows() {
		List<String> lines = getExaggeratedBigChunkyFile();
				
		
		Instant start = Instant.now();
		Set<String> hashes = lines.stream()
			.map(line -> {
				try {
					MessageDigest digest = MessageDigest.getInstance("MD5");
					return new String(digest.digest(line.getBytes()));
				} catch (Exception e) {
					throw new RuntimeException(e);
				}				
			})
			.collect(Collectors.toSet());
				
		Instant stop = Instant.now();
		System.out.println(String.format("hashRows took %d ms to finish", stop.toEpochMilli() - start.toEpochMilli()));
		return hashes;
	}

	private List<String> getExaggeratedBigChunkyFile() {
		List<String> lines;
		try {
			lines = Files.readAllLines(new File("src/main/resources/datensatz_gross.csv").toPath());		
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
		List<String> manyLines = new ArrayList<>(10000000);
		for(int i = 0; i < 1000; i++) {
			manyLines.addAll(lines);
		}
		
		return manyLines;
	}

	class Row {
		private String firstname;
		private String lastname;

		public Row(String csvRow) {
			String[] tokens = csvRow.split(";");
			firstname = tokens[2].trim();
			lastname = tokens[3].trim();
		}

		public String getFirstname() {
			return firstname;
		}

		public String getLastname() {
			return lastname;
		}
	}
}
