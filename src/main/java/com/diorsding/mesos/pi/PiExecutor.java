package com.diorsding.mesos.pi;

import java.io.IOException;

import org.apache.log4j.Logger;
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

public class PiExecutor implements Executor {

    private final static Logger LOGGER = Logger.getLogger(PiExecutor.class);


    public void registered(ExecutorDriver driver, ExecutorInfo executorInfo, FrameworkInfo frameworkInfo,
            SlaveInfo slaveInfo) {
        LOGGER.info("Registered PinUserBoardExecutor on " + slaveInfo.getHostname());
    }

    public void reregistered(ExecutorDriver driver, SlaveInfo slaveInfo) {
        // TODO Auto-generated method stub

    }

    public void disconnected(ExecutorDriver driver) {
        // TODO Auto-generated method stub

    }

    public void launchTask(ExecutorDriver driver, TaskInfo task) {
        LOGGER.info("Launching task in PinUserBoardExecutor..");
        TaskStatus taskStatus =
                TaskStatus.newBuilder().setTaskId(task.getTaskId()).setState(TaskState.TASK_RUNNING).build();
        driver.sendStatusUpdate(taskStatus);

        byte[] message = new byte[0];

        try {
            message = computePi().getBytes();
        } catch (IOException e) {
            LOGGER.error("Error computing Pi :" + e.getMessage());
        }
        LOGGER.info("Sending framework message and marking task finished." + getClass().getName());
        driver.sendFrameworkMessage(message);

        taskStatus = TaskStatus.newBuilder().setTaskId(task.getTaskId()).setState(TaskState.TASK_FINISHED).build();

        driver.sendStatusUpdate(taskStatus);

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
        MesosExecutorDriver mesosExecutorDriver = new MesosExecutorDriver(new PiExecutor());
        System.exit(mesosExecutorDriver.run() == Status.DRIVER_STOPPED ? 0 : 1);
    }

    /* Code to compute Pi */

    private String computePi() throws IOException {

        double pi = 0;
        double y = 1;

        int lps = 90000000 * 2;
        int cnt = 0;
        for (int x = 1; x < lps; x += 2) {
            pi = pi + (y / x);
            y = -y;
            cnt++;
        }

        return "Value of PI=" + 4 * pi + " after " + cnt;
        /* PI=3.141592642478473 after 90000000 */

    }

}
