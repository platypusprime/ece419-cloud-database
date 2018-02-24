package common;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Provides utility methods for handling hashing operations.
 */
public class HashUtil {

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

}
