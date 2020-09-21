package storage.test;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;

import org.junit.Test;

import storage.FileManager;
import storage.SlottedPageFile;
import storage.StorageManager.InvalidLocationException;

/**
 * This program tests the {@link FileManager} class.
 * 
 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
 * 
 */
public class FileManagerTest {

	static int additions = 1000;

	static int lookups = 500;

	static int removals = 10;

	/**
	 * Tests {@link FileManager#put(int, Long, Object)}.
	 * 
	 * @throws Exception
	 *             if an error occurs
	 */
	@Test
	public void put() throws Exception {
		FileManager m = initialize(FileManager.class);
		long[] locations = add(m, additions);
		ArrayList<Integer> list = new ArrayList<Integer>();
		for (int i = 0; i < 10; i++)
			list.add(i);
		for (Integer i : list) {
			m.put(0, concatenate(0, i), i);
			m.put(0, concatenate(1, i), i);
		}
		try {
			m.put(0, concatenate(-1, 0), 0);
			fail("expecting an " + InvalidLocationException.class.getSimpleName());
		} catch (Exception e) {
			assertTrue(e instanceof InvalidLocationException);
		}
		m.shutdown();
		SlottedPageFile f = new SlottedPageFile(0 + ".dat");
		assertEquals(list, SlottedPageTest.list(f.get(0).iterator()));
		assertEquals(list, SlottedPageTest.list(f.get(1).iterator()));
		f.close();
	}

	/**
	 * Tests {@link FileManager#get(int, Long)}.
	 * 
	 * @throws Exception
	 *             if an error occurs
	 */
	@Test
	public void get() throws Exception {
		FileManager m = initialize(FileManager.class);
		long[] locations = add(m, additions);
		try {
			m.get(0, concatenate(-1, 0));
			fail("expecting an " + InvalidLocationException.class.getSimpleName());
		} catch (Exception e) {
			assertTrue(e instanceof InvalidLocationException);
		}
		try {
			m.get(0, concatenate(1000, 0));
			fail("expecting an " + InvalidLocationException.class.getSimpleName());
		} catch (Exception e) {
			assertTrue(e instanceof InvalidLocationException);
		}
		assertEquals(successfulLookups(m, locations, lookups), lookups);
		m.shutdown();
	}

	/**
	 * Tests {@link FileManager#remove(int, Long)}.
	 * 
	 * @throws Exception
	 *             if an error occurs
	 */
	@Test
	public void remove() throws Exception {
		FileManager m = initialize(FileManager.class);
		long[] locations = add(m, additions);
		try {
			m.remove(0, concatenate(-1, 0));
			fail("expecting an " + InvalidLocationException.class.getSimpleName());
		} catch (Exception e) {
			assertTrue(e instanceof InvalidLocationException);
		}
		try {
			m.remove(0, concatenate(1000, 0));
			fail("expecting an " + InvalidLocationException.class.getSimpleName());
		} catch (Exception e) {
			assertTrue(e instanceof InvalidLocationException);
		}
		remove(m, removals, locations);
		for (int i = 0; i < additions; i += additions / removals)
			assertEquals(m.get(0, locations[i]), null);
		m.shutdown();
	}

	/**
	 * Tests {@link FileManager#iterator(int)}.
	 * 
	 * @throws Exception
	 *             if an error occurs
	 */
	@Test
	public void iterator() throws Exception {
		FileManager m = initialize(FileManager.class);
		long[] locations = add(m, additions);
		remove(m, removals, locations);
		ArrayList<Object> list = SlottedPageTest.list(m.iterator(0));
		assertEquals(list.size(), additions - removals);
		Iterator<Object> it = list.iterator();
		int stepSize = additions / removals;
		for (int i = 0; i < additions; i++)
			if (i % stepSize != 0)
				assertEquals(i, it.next());
	}

	/**
	 * Constructs a {@link FileManager} and initializes it.
	 * 
	 * @param c
	 *            a sub-type of {@link FileManager}
	 * @param args
	 *            arguments needed for creating a {@link FileManager}
	 * @return a new {@link FileManager}.
	 * @throws InstantiationException
	 *             if a {@link FileManager} cannot be instantiated
	 * @throws IOException
	 *             if an I/O error occurs
	 */
	static FileManager initialize(Class<? extends FileManager> c, Object... args)
			throws InstantiationException, IOException {
		FileManager m = newInstance(c, args);
		m.clear(0);
		return m;
	}

	/**
	 * Adds a certain number of objects in a {@link FileManager}.
	 * 
	 * @param m
	 *            a {@link FileManager}
	 * @param additions
	 *            the number of additions
	 * @return the locations of the objects that are added
	 * @throws IOException
	 *             if an I/O error occurs
	 */
	static long[] add(FileManager m, int additions) throws IOException {
		long[] locations = new long[additions];
		for (int i = 0; i < additions; i++) // add integers
			locations[i] = m.add(0, i);
		return locations;
	}

	/**
	 * Performs lookup operations.
	 * 
	 * @param m
	 *            a {@link FileManager}
	 * @param locations
	 *            an array storing locations
	 * @param lookups
	 *            the number of lookup operations to perform
	 * @return the number of successful lookup operations
	 * @throws IOException
	 *             if an I/O error occurs
	 */
	static int successfulLookups(FileManager m, long[] locations, int lookups) throws IOException {
		double prob = 1.0 / locations.length * 10;
		Random r = new Random(0);
		int count = 0;
		for (int i = 0; i < lookups; i++) {
			int index = 0;
			// find a random index according to a skewed distribution
			while (index < locations.length - 1 && r.nextDouble() > prob)
				index++;
			try {
				Object o = m.get(0, locations[index]);
				if (index == (Integer) o) {
					count++;
				}
			} catch (InvalidLocationException e) {
				// cannot happen
			}
		}
		return count;
	}

	/**
	 * Removes objects from a {@link FileManager}.
	 * 
	 * @param m
	 *            a {@link FileManager}
	 * @param removals
	 *            the number of removals
	 * @param locations
	 *            an array storing locations of objects
	 * @throws IOException
	 *             if an I/O error occurs
	 */
	private static void remove(FileManager m, int removals, long[] locations) throws IOException {
		for (int i = 0; i < additions; i += additions / removals)
			try {
				m.remove(0, locations[i]);
			} catch (InvalidLocationException e) {
				// cannot happen
				e.printStackTrace();
			}
	}

	/**
	 * Creates a new {@link FileManager}.
	 * 
	 * @param c
	 *            a {@link FileManager} implementation
	 * @param args
	 *            the arguments for instantiating a {@link FileManager}
	 * @return a new {@link FileManager}
	 * @throws InstantiationException
	 *             if an instantiation exception occurs
	 */
	static FileManager newInstance(Class<? extends FileManager> c, Object... args) throws InstantiationException {
		try {
			return (FileManager) c.getConstructors()[0].newInstance(args);
		} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException | SecurityException e) {
			e.printStackTrace();
			throw new InstantiationException();
		}
	}

	/**
	 * Returns a {@code long} value obtaining by concatenating the given {@code int} values
	 * 
	 * @param i
	 *            an {@code int} value
	 * @param j
	 *            an {@code int} value
	 * @return a {@code long} value obtaining by concatenating the given {@code int} values
	 */
	protected long concatenate(int i, int j) {
		return (((long) i) << 32) | j;
	}

	/**
	 * Tests a {@link FileManager} by adding, removing, and accessing objects.
	 * 
	 * @param c
	 *            a sub-type of {@link FileManager}.
	 * @param out
	 *            a {@link PrintStream}
	 * @param args
	 *            arguments needed for constructing a {@link FileManager}
	 * @throws InstantiationException
	 *             if a {@link FileManager} cannot be instantiated
	 * @throws IOException
	 *             if an I/O error occurs
	 */
	public static void test(Class<? extends FileManager> c, PrintStream out, Object... args)
			throws InstantiationException, IOException {
		FileManager m = initialize(c, args);
		long[] locations = add(m, additions);
		out.println(locations.length + " additions % " + m);
		remove(m, removals, locations);
		out.println(removals + " removals % " + m);
		ArrayList<Object> list = SlottedPageTest.list(m.iterator(0));
		out.println("iteration over " + list.size() + " elements % " + m);
		m.shutdown();
		out.println("shut down % " + m);
	}

}
