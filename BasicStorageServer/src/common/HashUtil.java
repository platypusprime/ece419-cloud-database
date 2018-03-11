package common;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Provides utility methods for handling hashing operations.
 */
public class HashUtil {

	/** The smallest possible value for a valid MD5 hash. */
	public static final String MIN_MD5 = "00000000000000000000000000000000";

	/** The largest possible value for a valid MD5 hash. */
	public static final String MAX_MD5 = "ffffffffffffffffffffffffffffffff";

	/**
	 * Computes the MD5 hash for the given socket address.
	 * 
	 * @param host The hostname of the socket
	 * @param port The port number for the socket
	 * @return The computed MD5 hash, or <code>null</code>
	 *         if the given hostname was <code>null</code>
	 */
	public static String toMD5(String host, int port) {
		if (host == null) return null;
		return HashUtil.toMD5(host + ":" + port);
	}

	/**
	 * Computes the MD5 hash for a given string.
	 * 
	 * @param s The string to hash
	 * @return The computed MD5 hash, or <code>null</code> if
	 *         the incoming string was <code>null</code>
	 */
	public static String toMD5(String s) {
		if (s == null) return null;

		MessageDigest md;
		byte[] mdbytes;

		try {
			md = MessageDigest.getInstance("MD5");
			mdbytes = md.digest(s.getBytes("UTF-8"));
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}

		// convert the byte to hex format
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < mdbytes.length; i++) {
			sb.append(Integer.toString((mdbytes[i] & 0xff) + 0x100, 16).substring(1));
		}

		return sb.toString();
	}

	/**
	 * Checks whether the given string is a valid MD5 hash. Specifically checks
	 * whether the string contains a 32-digit hexadecimal number.
	 * 
	 * @param hash The string to check
	 * @return <code>true</code> if the given string is a 32-digit hexadecimal
	 *         number, <code>false</code> otherwise
	 */
	public static boolean validateHash(String hash) {
		return hash != null && hash.matches("[0-9a-f]{32}");
	}

}
