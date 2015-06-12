package edu.uci.ics.hyracks.api.util;

import java.text.SimpleDateFormat;
import java.util.Calendar;

public class OperatorExecutionTimeProfiler {
    public static final OperatorExecutionTimeProfiler INSTANCE = new OperatorExecutionTimeProfiler();
    public ExperimentProfiler executionTimeProfiler;

    private OperatorExecutionTimeProfiler() {
        if (ExperimentProfiler.PROFILE_MODE) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss-SSS");
            executionTimeProfiler = new ExperimentProfiler("executionTime" + sdf.format(Calendar.getInstance().getTime()) + ".txt", 1);
            executionTimeProfiler.begin();
        }
    }
}
