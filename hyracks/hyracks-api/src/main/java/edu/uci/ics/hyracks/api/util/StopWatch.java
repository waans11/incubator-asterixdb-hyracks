package edu.uci.ics.hyracks.api.util;

import java.text.SimpleDateFormat;

public class StopWatch {
	private long startTime = 0;
	private long stopTime = 0;
	private long elapsedTime = 0;

	private long elapsedTimeBetweenTimeStamp = 0;

	// starting timestamp of an operator
	private long startTimeStamp = 0;

	// ending timestamp
	private long endTimeStamp = 0;

	// The timer has started?
	private boolean isStarted = false;

	private String message;

	public void start() {
		elapsedTime = 0;
		startTime = System.currentTimeMillis();
		startTimeStamp = startTime;
		isStarted = true;
		message = "";
	}

	public void suspend() {
		stopTime = System.currentTimeMillis();
		elapsedTime += stopTime - startTime;
	}

	public void resume() {
		startTime = System.currentTimeMillis();
	}

	public void finish() {
		endTimeStamp = stopTime;
		elapsedTimeBetweenTimeStamp = endTimeStamp - startTimeStamp;
	}

	// elapsed time in milliseconds
	public long getElapsedTime() {
		return elapsedTime;
	}

	// elapsed time in seconds
	public double getElapsedTimeSecs() {
		return (double) elapsedTime / 1000;
	}

	// elapsed time in milliseconds
	public long getElapsedTimeStamp() {
		return elapsedTimeBetweenTimeStamp;
	}

	// elapsed time in seconds
	public double getElapsedTimeStampSecs() {
		return (double) elapsedTimeBetweenTimeStamp / 1000;
	}

	public String getMessage(String operatorName, long timeStamp) {
		message = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS")
				.format(timeStamp)
				+ "\t"
				+ operatorName
				+ "\t"
				+ this.getElapsedTime()
				+ "\t"
				+ this.getElapsedTimeSecs()
				+ "\t"
				+ this.getElapsedTimeStamp()
				+ "\t"
				+ this.getElapsedTimeStampSecs()
				+ "\n";
		return message;
	}

	public long getStartTimeStamp() {
		return startTimeStamp;
	}

	public long getEndTimeStamp() {
		return endTimeStamp;
	}

}