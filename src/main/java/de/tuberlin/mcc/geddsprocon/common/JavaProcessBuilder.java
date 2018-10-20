package de.tuberlin.mcc.geddsprocon.common;

import java.io.File;
import java.io.IOException;

public class JavaProcessBuilder {

    public static Process exec(Class javaClass) throws IOException, InterruptedException {
        return exec(javaClass, "ipc:///message-buffer-process", false);
    }

    public static Process exec(Class javaClass, String connectionString, boolean addSentMessagesFrame) throws IOException, InterruptedException {
        String javaHome = System.getProperty("java.home");
        String javaBin = javaHome +
                File.separator + "bin" +
                File.separator + "java";
        String classpath = System.getProperty("java.class.path");
        String className = javaClass.getCanonicalName();

        ProcessBuilder builder = new ProcessBuilder(
                javaBin, "-cp", classpath, className, connectionString, Boolean.toString(addSentMessagesFrame));

        builder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        builder.redirectError(ProcessBuilder.Redirect.INHERIT);

        Process process = builder.start();
        return process;
    }
}
