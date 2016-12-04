package com.diorsding.mesos.rendler;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.FrameworkInfo;
import org.apache.mesos.Protos.SlaveInfo;
import org.apache.mesos.Protos.Status;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.TaskState;
import org.apache.mesos.Protos.TaskStatus;

public class RenderExecutor implements Executor {

    String currentPath;

    public void registered(ExecutorDriver driver, ExecutorInfo executorInfo, FrameworkInfo frameworkInfo,
            SlaveInfo slaveInfo) {
        System.out.println("Registered executor on " + slaveInfo.getHostname());
        currentPath = executorInfo.getData().toStringUtf8();
        currentPath = "/vagrant/";
    }

    public void reregistered(ExecutorDriver driver, SlaveInfo slaveInfo) {
        // TODO Auto-generated method stub

    }

    public void disconnected(ExecutorDriver driver) {
        // TODO Auto-generated method stub

    }

    public void launchTask(ExecutorDriver driver, TaskInfo taskInfo) {
        // Start task with status running
        TaskStatus status =
                TaskStatus.newBuilder().setTaskId(taskInfo.getTaskId()).setState(TaskState.TASK_RUNNING).build();
        driver.sendStatusUpdate(status);

        String url = taskInfo.getData().toStringUtf8();
        String renderJSPath = currentPath + "/render.js";
        String workPathDir = currentPath + "/rendleroutput/";
        // Run phantom js
        String filename = workPathDir + taskInfo.getTaskId().getValue() + ".png";
        String cmd = "phantomjs " + renderJSPath + " " + url + " " + filename;

        try {
            runProcess(cmd);
            // If successful, send the framework message
            if (new File(filename).exists()) {
                String myStatus = "render" + url + "," + filename;
                driver.sendFrameworkMessage(myStatus.getBytes());
            }

        } catch (Exception e) {
            System.out.println("Exception executing phantomjs: " + e);
        }

        // Set the task with status finished
        status = TaskStatus.newBuilder().setTaskId(taskInfo.getTaskId()).setState(TaskState.TASK_FINISHED).build();
        driver.sendStatusUpdate(status);

    }

    public void killTask(ExecutorDriver driver, TaskID taskId) {
        // TODO Auto-generated method stub

    }

    public void frameworkMessage(ExecutorDriver driver, byte[] data) {
        // TODO Auto-generated method stub

    }

    public void shutdown(ExecutorDriver driver) {
        // TODO Auto-generated method stub

    }

    public void error(ExecutorDriver driver, String message) {
        // TODO Auto-generated method stub

    }


    public static void main(String[] args) {
        MesosExecutorDriver driver = new MesosExecutorDriver(new RenderExecutor());
        System.exit(driver.run() == Status.DRIVER_STOPPED ? 0 : 1);
    }


    /**
     * Print lines for any input stream, i.e. stdout or stderr.
     *
     * @param name the label for the input stream
     * @param ins the input stream containing the data
     */
    private void printLines(String name, InputStream ins) throws Exception {
        String line = null;
        BufferedReader in = new BufferedReader(new InputStreamReader(ins));
        while ((line = in.readLine()) != null) {
            System.out.println(name + " " + line);
        }
    }

    /**
     * Execute a command with error logging.
     *
     * @param the string containing the command that needs to be executed
     */
    private void runProcess(String command) throws Exception {
        Process pro = Runtime.getRuntime().exec(command);
        printLines(command + " stdout:", pro.getInputStream());
        printLines(command + " stderr:", pro.getErrorStream());
        pro.waitFor();

        System.out.println(command + " exitValue() " + pro.exitValue());
    }
}
