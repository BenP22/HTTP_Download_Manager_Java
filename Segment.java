

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Segment implements Serializable {

	/**
	 * The state of the segment
	 */
	public enum SegmentState {
		AVAILABLE,
		ALLOCATED,
		IN_PROGRESS,
		FINISHED_PRODUCING,
	}

	private static final long serialVersionUID = 598341237472344L;
	private long startIndex;
	private long endIndex;
	private SegmentState state;

	/**
	 * @param startIndex - index that starts the segment
	 * @param endIndex - index that ends the segment
	 */
	public Segment(long startIndex, long endIndex) {
		this.startIndex = startIndex;
		this.endIndex = endIndex;
		this.state = SegmentState.AVAILABLE;
	}

	/**
	 * @param segments - segments to inflate
	 * @param factor - factor to inflate by
	 * @return - given segments split by the given factor
	 */
	public static List<Segment> InflateSegments(List<Segment> segments, int factor) {
		if (factor < 1 || segments.size() > factor) {
			return segments;
		}

		ArrayList<Segment> inflated = new ArrayList<>();

		for (Segment segment : segments) {
			// Split the segment into two even parts
			Segment segment1 = new Segment(segment.startIndex, segment.startIndex + ((segment.endIndex - segment.startIndex) / 2));
			Segment segment2 = new Segment(segment.startIndex + ((segment.endIndex - segment.startIndex) / 2), segment.endIndex);

			// Add the segments into the result
			inflated.add(segment1);
			inflated.add(segment2);
		}

		// Recurse until reaching desired factor of inflation
		return InflateSegments(inflated, factor - 1);
	}

	/**
	 * @param segments - segments to get one from
	 * @return - one of the segments that should be processed
	 */
	public static Segment GetOne(List<Segment> segments) {
		for (Segment segment : segments) {
			if (segment.state == SegmentState.AVAILABLE || segment.state == SegmentState.IN_PROGRESS) {
				// we want to get only segments that are not finished
				if (segment.startIndex < segment.endIndex) {
					segment.state = SegmentState.ALLOCATED;
					return segment;
				} else {
					segment.setState(SegmentState.FINISHED_PRODUCING);
				}
			}
		}

		return null;
	}

	/**
	 * @param start - start index to calculate segments by
	 * @param end - end index to calculate segments by
	 * @param segmentSize - size of a segment
	 * @return - list of segments from the given interval
	 */
	public static List<Segment> GetSegments(long start, long end, long segmentSize) {
		ArrayList<Segment> segmentsArrayList = new ArrayList<>();

		while (start < end) {
			// so we would not overflow the last segment
			long proposedEnd = start + segmentSize;
			if (proposedEnd > end) {
				proposedEnd = end;
			}

			segmentsArrayList.add(new Segment(start, proposedEnd));
			start = proposedEnd;
		}

		return segmentsArrayList;
	}

	/**
	 * @param list - a list of segments
	 * @param numOfParts - number of parts to partition by
	 * @param <Segment> - type
	 * @return - a partitioned list of lists of segments, that can be given to numOfParts workers and each will have
 * 				a fair size
	 */
	public static <Segment> List<List<Segment>> PartitionSegments(List<Segment> list, int numOfParts) {
		List<List<Segment>> partsList = new ArrayList<List<Segment>>();
		int chunkSize = list.size() / numOfParts;
		int leftOver = list.size() % numOfParts;
		int take = chunkSize;

		for (int i = 0, iT = list.size(); i < iT; i += take) {
			// when leftOVer is positive, reduce it and increase chunk
			if (leftOver > 0) {
				leftOver--;

				take = chunkSize + 1;
			} else {
				take = chunkSize;
			}

			partsList.add(new ArrayList<Segment>(list.subList(i, Math.min(iT, i + take))));
		}

		return partsList;
	}

	/**
	 * @param segments - segments to compress
	 * @return a compressed list of segments that should reduce the number of segment without losing intervals
	 */
	public static List<Segment> CompressSegments(List<Segment> segments) {
		List<Segment> result = new ArrayList<>();

		if (segments.size() < 1) {
			return result;
		}
		result.add(segments.get(0)); // add the first segment

		int j = 1;
		for (int i = 1; i < segments.size(); i++) {
			// if last segment has the same end, we can compress them into one
			if (result.get(j - 1).endIndex == segments.get(i).startIndex) {

				result.add(new Segment(result.get(j - 1).startIndex, segments.get(i).endIndex));
				result.remove(j - 1);

			} else {
				result.add(segments.get(i));
				j++;
			}
		}

		return result;
	}

	public long getStartIndex() {
		return startIndex;
	}

	public void setStartIndex(long startIndex) {
		this.startIndex = startIndex;
	}

	public long getEndIndex() {
		return endIndex;
	}

	public SegmentState getState() {
		return state;
	}

	public void setState(SegmentState state) {
		this.state = state;
	}

	@Override
	public String toString() {
		return "Segment{" +
				"startIndex=" + startIndex +
				", endIndex=" + endIndex +
				", state=" + state +
				'}';
	}
}
