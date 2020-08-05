package com.kesque.pulsar.sink.s3;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;

public class Util {

    public static String ensureValidBucketName(String bucketName) {
		String formatted = bucketName.replaceAll("\\s+","_");
		int length = bucketName.length();
		if(length >= 62)
			length = 62;
		formatted = formatted.substring(0,length);
		formatted = formatted.replace(".","d");
		formatted = formatted.toLowerCase();
		if(formatted.endsWith("-"))
			formatted = formatted.substring(0,length - 1);
		
		return formatted;
	}

	public static String getHourlyTimestamp(long epoch) {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd-HH");
		return format.format(epoch);
	}

	public static String getMinuteTimestamp(long epoch) {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd-HH-mm");
		return format.format(epoch);
	}

	/**
	 * Check if the current time is over the duration limite since the start.
	 * @param start
	 * @param limit
	 * @return
	 */
	public static boolean isOver(Instant start, Duration limit) {
		return Duration.between(start, Instant.now()).compareTo(limit) > 0;
	}
}