package edu.uci.ics.hyracks.api.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class ExperimentProfilerUtils {
    public static void printToOutputFile(StringBuffer sb, FileOutputStream fos) throws IllegalStateException,
            IOException {
        fos.write(sb.toString().getBytes());
    }

    public static FileOutputStream openOutputFile(String filepath) throws IOException {
        File file = new File(filepath);
        if (!file.exists()) {
//            file.delete();
            file.createNewFile();
        }
        return new FileOutputStream(file);
    }

    public static void closeOutputFile(FileOutputStream fos) throws IOException {
        fos.flush();
        fos.close();
        fos = null;
    }
}
