package com.diorsding.mesos.pi;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.MasterInfo;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.TaskState;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.mesos.Protos.Value;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;


public class PiScheduler implements Scheduler {

    private final static Logger LOGGER = Logger.getLogger(PiScheduler.class);

    private final ExecutorInfo piExecutor;
    private final int totalTasks;
    private int launchedTasks = 0;
    private int finishedTasks = 0;

    public PiScheduler(ExecutorInfo piExecutor, int totalTasks) {
        this.piExecutor = piExecutor;
        this.totalTasks = totalTasks;
    }

    public void registered(SchedulerDriver driver, FrameworkID frameworkId, MasterInfo masterInfo) {
        LOGGER.info("Registered! ID = " + frameworkId.getValue());

    }

    public void reregistered(SchedulerDriver driver, MasterInfo masterInfo) {
        // TODO Auto-generated method stub

    }

    public void resourceOffers(SchedulerDriver driver, List<Offer> offers) {
        double CPUS_PER_TASK = 1;
        double MEM_PER_TASK = 128;

        for (Offer offer : offers) {
            List<TaskInfo> taskInfoList = new ArrayList<TaskInfo>();
            double offerCpus = 0;
            double offerMem = 0;

            for (Resource resource : offer.getResourcesList()) {
                if (resource.getName().equals("cpus")) {
                    offerCpus += resource.getScalar().getValue();
                } else if (resource.getName().equals("mem")) {
                    offerMem += resource.getScalar().getValue();
                }
            }

            LOGGER.info("Received Offer : " + offer.getId().getValue() + "with cpus = " + offerCpus + " and mem ="
                    + offerMem);

            double remainingCpus = offerCpus;
            double remainingMem = offerMem;

            if (launchedTasks < totalTasks && remainingCpus >= CPUS_PER_TASK && remainingMem >= MEM_PER_TASK) {
                TaskID taskID = TaskID.newBuilder().setValue(Integer.toString(launchedTasks++)).build();
                LOGGER.info("Launching task :" + taskID.getValue() + " using the offer : " + offer.getId().getValue());

                /* PiExecutor is created as a task and is launched */

                TaskInfo piTaskInfo =
                        TaskInfo.newBuilder()
                                .setName("task " + taskID.getValue())
                                .setTaskId(taskID)
                                .setSlaveId(offer.getSlaveId())
                                .addResources(
                                        Resource.newBuilder()
                                        .setName("cpus")
                                        .setType(Value.Type.SCALAR)
                                        .setScalar(Value.Scalar.newBuilder().setValue(CPUS_PER_TASK)))
                                .addResources(
                                        Resource.newBuilder()
                                        .setName("mem")
                                        .setType(Value.Type.SCALAR)
                                        .setScalar(Value.Scalar.newBuilder().setValue(MEM_PER_TASK)))
                                .setExecutor(ExecutorInfo.newBuilder(piExecutor)).build();

                taskID = TaskID.newBuilder().setValue(Integer.toString(launchedTasks++)).build();

                LOGGER.info("Launching task :" + taskID.getValue() + " using the offer : " + offer.getId().getValue());

                taskInfoList.add(piTaskInfo);
            }

            driver.launchTasks(offer.getId(), taskInfoList);
        }
    }

    public void offerRescinded(SchedulerDriver driver, OfferID offerId) {
        // TODO Auto-generated method stub

    }

    public void statusUpdate(SchedulerDriver schedulerDriver, TaskStatus taskStatus) {
        LOGGER.info("Status update : Task ID " + taskStatus.getTaskId().getValue() + "in state : "
                + taskStatus.getState().getValueDescriptor().getName());
        if (taskStatus.getState() == TaskState.TASK_FINISHED) {
            finishedTasks++;
            LOGGER.info("Finished tasks : " + finishedTasks);

            /* We can stop the scheduler once the tasks are completed */
            if (finishedTasks == totalTasks) {
                schedulerDriver.stop();
            }
        }

        if (taskStatus.getState() == TaskState.TASK_FAILED || taskStatus.getState() == TaskState.TASK_KILLED
                || taskStatus.getState() == TaskState.TASK_LOST) {
            LOGGER.error("Aborting because the task " + taskStatus.getTaskId().getValue()
                    + " is in unexpected state : " + taskStatus.getState().getValueDescriptor().getName()
                    + "with reason : " + taskStatus.getReason().getValueDescriptor().getName() + " from source : "
                    + taskStatus.getSource().getValueDescriptor().getName() + " with message : "
                    + taskStatus.getMessage());
            schedulerDriver.abort();
        }

    }

    public void frameworkMessage(SchedulerDriver driver, ExecutorID executorId, SlaveID slaveId, byte[] data) {
        String dataStr = new String(data);
        System.out.println(dataStr);
        LOGGER.info("Output :\n=========\n " + dataStr);

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
        LOGGER.error("Error : " + message);

    }

}
