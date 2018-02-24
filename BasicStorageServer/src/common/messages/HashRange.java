package common.messages;

/**
 * Represents a range of hash values on a hash ring going from <code>0x0</code>
 * to <code>0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF</code>. Ranges can wrap around
 * these limits.
 */
public class HashRange {

	/** The JSON attribute name for the hash range start index. */
	public static final String START_ATTR = "startIndex";

	/** The JSON attribute name for the hash range end index. */
	public static final String END_ATTR = "endIndex";

	private String start;
	private String end;

	/**
	 * Creates a new hash range descending from <code>start</code> to
	 * <code>end</code>.
	 * 
	 * @param start The hash value of the start index (inclusive)
	 * @param end The hash value of the end index (exclusive)
	 */
	public HashRange(String start, String end) {
		this.setStart(start);
		this.setEnd(end);
	}

	/**
	 * Returns the start index of this range.
	 * 
	 * @return The start index (inclusive)
	 */
	public String getStart() {
		return start;
	}

	/**
	 * Sets the start index of this range.
	 * 
	 * @param start The start index to set
	 * @throws IllegalArgumentException If the given index is not a valid MD5 hash
	 */
	public void setStart(String start) throws IllegalArgumentException {
		if (!validateHash(start))
			throw new IllegalArgumentException("Start value \"" + start + "\" is not a valid MD5 hash");
		this.start = start;
	}

	/**
	 * Returns the end index of this range.
	 * 
	 * @return The end index (exclusive)
	 */
	public String getEnd() {
		return end;
	}

	/**
	 * Sets the end index of this range.
	 * 
	 * @param end The end index to set
	 * @throws IllegalArgumentException If the given index is not a valid MD5 hash
	 */
	public void setEnd(String end) throws IllegalArgumentException {
		if (!validateHash(end))
			throw new IllegalArgumentException("End value \"" + end + "\" is not a valid MD5 hash");
		this.end = end;
	}

	/**
	 * Checks whether the given string is a valid MD5 hash. Specifically checks
	 * whether the string contains a 32-digit hexadecimal number.
	 * 
	 * @param hash The string to check
	 * @return <code>true</code> if the given string is a 32-digit hexadecimal
	 *         number, <code>false</code> otherwise
	 */
	public boolean validateHash(String hash) {
		return hash != null && hash.matches("[0-9a-f]{32}");
	}

	/**
	 * Checks whether the given MD5 hash belongs within this hash range.
	 * 
	 * @param hash The hash string to check
	 * @return <code>true</code> if <code>hash</code> lies within this range's start
	 *         index (inclusive) and end index (exclusive) with consideration for
	 *         wrap-around, <code>false</code> otherwise
	 */
	public boolean containsHash(String hash) {
		validateHash(hash);

		if (start.compareTo(end) > 0) {
			// no wrap-around
			return hash.compareTo(start) <= 0 && hash.compareTo(end) > 0;
		} else {
			// wrap-around
			return hash.compareTo(start) <= 0 || hash.compareTo(end) > 0;
		}

	}
}