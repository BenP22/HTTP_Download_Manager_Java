

public class SegmentPayload {

	private Segment segment;
	private byte[] data;
	private int length;
	private long seek;

	/**
	 * @param segment - the related segment
	 * @param data - data to write to disk
	 * @param length - length of data
	 * @param seek - where in the file to seek before writing the data
	 */
	public SegmentPayload(Segment segment, byte[] data, int length, long seek) {

		this.segment = segment;
		this.data = data;
		this.length = length;
		this.seek = seek;
	}

	public Segment getSegment() {
		return segment;
	}

	public byte[] getData() {
		return data;
	}

	public int getLength() {
		return length;
	}

	public long getSeek() {
		return seek;
	}
}
