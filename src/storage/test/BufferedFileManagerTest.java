package storage.test;

import java.io.PrintStream;

import storage.BufferedFileManager;

/**
 * This program tests the {@code BufferedFileManager} class.
 * 
 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
 * 
 */
public class BufferedFileManagerTest {

	/**
	 * The main program.
	 * 
	 * @param args
	 *            the String arguments
	 * @throws Exception
	 *             if an error occurs
	 */
	public static void main(String[] args) throws Exception {
		test(4, System.out);
		test(16, System.out);
		test(64, System.out);
	}

	/**
	 * Tests the {@code BufferedFileManager} implementation using the specified buffer size.
	 * 
	 * @param bufferSize
	 *            the buffer size
	 * @param out
	 *            a {@code PrintStream}
	 * @throws Exception
	 *             if an error occurs
	 */
	static void test(int bufferSize, PrintStream out) throws Exception {
		out.println("buffer size: " + bufferSize + " pages");
		FileManagerTest.test(BufferedFileManager.class, out, bufferSize);
		out.println();
	}

}
