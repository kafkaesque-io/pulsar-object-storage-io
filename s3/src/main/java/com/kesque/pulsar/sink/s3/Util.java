package com.kesque.pulsar.sink.s3;

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
}