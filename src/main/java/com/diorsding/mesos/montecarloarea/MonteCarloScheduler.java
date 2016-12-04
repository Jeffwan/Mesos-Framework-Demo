package com.diorsding.mesos.montecarloarea;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.mesos.Protos;
import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.MasterInfo;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

public class MonteCarloScheduler implements Scheduler {
    // Internal Queue to hold tasks to be processed.
    private LinkedList<String> tasks;
    private int numTasks;
    private int tasksSubmitted;
    private int tasksCompleted;
    private double totalArea;

    /**
     * Job will be divide equally. So this particular task, every part is same.
     *
     * @param args
     * @param numTasks
     */

    public MonteCarloScheduler(String[] args, int numTasks) {
        this.numTasks = numTasks;
        tasks = new LinkedList<String>();
        double xLow = Double.parseDouble(args[1]);
        double xHigh = Double.parseDouble(args[2]);
        double yLow = Double.parseDouble(args[3]);
        double yHigh = Double.parseDouble(args[4]);
        double xStep = (xHigh - xLow) / (numTasks / 2);
        double yStep = (yHigh - yLow) / (numTasks / 2);
        for (double x = xLow; x < xHigh; x += xStep) {
            for (double y = yLow; y < yHigh; y += yStep) {
                tasks.add(" \"" + args[0] + "\" " + x + " " + (x + xStep) + " " + y + " " + (y + yStep) + " "
                        + args[5]);
            }
        }
    }

    public void registered(SchedulerDriver driver, FrameworkID frameworkId, MasterInfo masterInfo) {
        System.out.println("Scheduler registered with id " + frameworkId.getValue());

    }

    public void reregistered(SchedulerDriver driver, MasterInfo masterInfo) {
        System.out.println("Scheduler re-registered");

    }

    public void resourceOffers(SchedulerDriver driver, List<Offer> offers) {
        for (Protos.Offer offer : offers) {
            if (tasks.size() > 0) {
                tasksSubmitted++;
                String task = tasks.remove();
                Protos.TaskID taskID = Protos.TaskID.newBuilder().setValue(String.valueOf(tasksSubmitted)).build();
                System.out.println("Launching task " + taskID.getValue() + " on slave " + offer.getSlaveId().getValue()
                        + " with " + task);

                // Easily find the slaveId which provides this resource offer.
                // Set executor running command
                Protos.ExecutorInfo executor =
                        Protos.ExecutorInfo.newBuilder()
                                .setExecutorId(Protos.ExecutorID.newBuilder().setValue(String.valueOf(tasksSubmitted)))
                                .setCommand(createCommand(task)).setName("MonteCarlo Executor (Java)").build();

                // Assign tasks based on the offer. Single tasks can not eat up all the resource offer provides.
                Protos.TaskInfo taskInfo =
                        Protos.TaskInfo
                                .newBuilder()
                                .setName("MonteCarloTask-" + taskID.getValue())
                                .setTaskId(taskID)
                                .setExecutor(Protos.ExecutorInfo.newBuilder(executor))
                                .addResources(
                                        Protos.Resource.newBuilder().setName("cpus").setType(Protos.Value.Type.SCALAR)
                                                .setScalar(Protos.Value.Scalar.newBuilder().setValue(1)))
                                .addResources(
                                        Protos.Resource.newBuilder().setName("mem").setType(Protos.Value.Type.SCALAR)
                                                .setScalar(Protos.Value.Scalar.newBuilder().setValue(128)))
                                .setSlaveId(offer.getSlaveId()).build();
                driver.launchTasks(Collections.singletonList(offer.getId()),
                        Collections.singletonList(taskInfo));
            }
        }

    }

    private Protos.CommandInfo.Builder createCommand(String args) {
        String jarPath = System.getProperty("JAR_PATH");
        jarPath = "/vagrant/mesos-framework-demo.jar";
        String command =
                "java -cp " + jarPath + " com.diorsding.mesos.montecarloarea.MonteCarloExecutor" + args;

        System.out.println(command);

        return Protos.CommandInfo.newBuilder().setValue(command);
    }

    public void offerRescinded(SchedulerDriver driver, OfferID offerId) {
        // TODO Auto-generated method stub

    }

    // Event based - We can handle statusChange in single scheduler. Make works easy.
    public void statusUpdate(SchedulerDriver schedulerDriver, TaskStatus taskStatus) {
        System.out.println("Status update: task " + taskStatus.getTaskId().getValue() + " state is "
                + taskStatus.getState());

        if (taskStatus.getState().equals(Protos.TaskState.TASK_FINISHED)) {
            tasksCompleted++;
            double area = Double.parseDouble(taskStatus.getData().toStringUtf8());
            totalArea += area;
            System.out.println("Task " + taskStatus.getTaskId().getValue() + " finished with area : " + area);
        } else {
            System.out.println("Task " + taskStatus.getTaskId().getValue() + " has message " + taskStatus.getMessage());
        }

        if (tasksCompleted == numTasks) {
            System.out.println("Total Area : " + totalArea);
            schedulerDriver.stop();
        }
    }

    public void frameworkMessage(SchedulerDriver driver, ExecutorID executorId, SlaveID slaveId, byte[] data) {
        // TODO Auto-generated method stub

    }

    public void disconnected(SchedulerDriver driver) {
        // TODO Auto-generated method stub

    }

    public void slaveLost(SchedulerDriver driver, SlaveID slaveId) {
        // TODO Auto-generated method stub

    }

    public void executorLost(SchedulerDriver driver, ExecutorID executorId, SlaveID slaveId, int status) {
        // TODO Auto-generated method stub

    }

    public void error(SchedulerDriver driver, String message) {
        System.out.println("Error : " + message);
    }

}
