package cdstreams;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class FunWithStreamsTest {

	private FunWithStreams fun;

	@Before
	public void setup() {
		fun = new FunWithStreams();
	}
	
	@Test
	public void should_return_directories() throws Exception {
		List<File> dirs = fun.getDirs();
		assertEquals(2, dirs.size());
		for (File file : dirs) {
			assertTrue(file.isDirectory());
		}
	}
	
	@Test
	public void should_get_non_empty_directories() throws Exception {
		List<File> nonEmptyDirs = fun.getNonEmptyDirs();
		assertEquals(1, nonEmptyDirs.size());
	}
	
	@Test
	public void should_contains_directories() throws Exception {
		assertTrue(fun.containsDirs());
	}
	

	@Test
	public void should_contains_only_files() throws Exception {
		assertFalse(fun.containsOnlyFiles());
	}	
	
	@Test
	public void should_find_file_by_name() throws Exception {
		assertFalse(fun.findFile("foo.bar").isPresent());
		
		Optional<File> file = fun.findFile("datensatz_klein.csv");
		assertTrue(file.isPresent());
	}
	
	@Test
	public void should_get_all_filenames() throws Exception {
		List<String> filenames = fun.getFilenames();
		assertEquals(5, filenames.size());
	}

	@Test
	public void should_calculate_sum_of_filesize() throws Exception {
		long filesize = fun.getSumFilesize();
		assertEquals(1135184, filesize);
	}
	
	@Test
	public void should_count_firstnames() throws Exception {
		Map<String, Long> countFirstnames = fun.countFirstnames();
		assertTrue(countFirstnames.get("Paula") >= 1);
	}
	
	@Test
	@Ignore
	public void should_hash_rows() throws Exception {
		Set<String> hashes = fun.hashRows();
		assertNotNull(hashes);
	}
}
