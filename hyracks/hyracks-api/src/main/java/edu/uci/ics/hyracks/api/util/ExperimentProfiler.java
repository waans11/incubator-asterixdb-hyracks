package edu.uci.ics.hyracks.api.util;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class ExperimentProfiler {

    public static final boolean PROFILE_MODE = true;
    private FileOutputStream fos;
    private String filePath;
    private StringBuilder sb;
    private int printInterval;
    private int addCount;
    private Object lock1 = new Object();

//    private HashMap<String, profiledTimeValue> spentTimePerOperatorMap;

    // [Key: Job, Value: [Key: Operator, Value: Duration of each operators]]
    private HashMap<String, LinkedHashMap<String, String>> spentTimePerJobMap;

    public ExperimentProfiler(String filePath, int printInterval) {
        this.filePath = new String(filePath);
        this.sb = new StringBuilder();
        this.printInterval = printInterval;
        this.spentTimePerJobMap = new HashMap<String, LinkedHashMap<String, String>>();
    }

    public void begin() {
        try {
            fos = ExperimentProfilerUtils.openOutputFile(filePath);
            addCount = 0;
        } catch (IOException e) {
            e.printStackTrace();
            throw new IllegalStateException(e);
        }
    }

    public synchronized void add(String jobSignature, String operatorSignature, String message, boolean flushNeeded) {

    	// First, check whether the job is in the hash-map or not.
    	// If so, insert the duration of an operator to the hash map
    	if (!spentTimePerJobMap.containsKey(jobSignature)) {
    		spentTimePerJobMap.put(jobSignature, new LinkedHashMap<String, String>());
    	}
		spentTimePerJobMap.get(jobSignature).put(operatorSignature, message);

//		spentTimePerJobMap.put(operatorSignature, message);
//        sb.append(s);
        if (flushNeeded) {
            flush(jobSignature);
        }
//        if (printInterval > 0 && ++addCount % printInterval == 0) {
//            flush();
//            addCount = 0;
//        }
    }

    public synchronized void flush(String jobSignature) {
        try {
        	synchronized (lock1) {
        		sb.append("\n\n");
            	for (Map.Entry<String, String> entry : spentTimePerJobMap.get(jobSignature).entrySet()) {
            		sb.append(entry.getValue());
            	}
                fos.write(sb.toString().getBytes());
                fos.flush();
                System.out.println("Flushing " + jobSignature);
            	spentTimePerJobMap.get(jobSignature).clear();
                sb.setLength(0);
			}
//            spentTimePerOperator.clear();
        } catch (IOException e) {
            e.printStackTrace();
            throw new IllegalStateException(e);
        }
    }

    public void clear() {
    	spentTimePerJobMap.clear();
        sb.setLength(0);
    }

    public void clear(String jobSignature) {
    	spentTimePerJobMap.get(jobSignature).clear();
        sb.setLength(0);
    }

    public synchronized void end() {
        try {
            if (fos != null) {
                fos.flush();
                fos.close();
                fos = null;
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new IllegalStateException(e);
        }
    }
}
