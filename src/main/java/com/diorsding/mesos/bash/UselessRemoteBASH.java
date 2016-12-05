package com.diorsding.mesos.bash;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.FrameworkInfo;
import org.apache.mesos.Protos.MasterInfo;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.json.JSONArray;
import org.json.JSONObject;

public class UselessRemoteBASH implements Scheduler {

    private List<Job> jobs = new ArrayList<Job>();

    public void registered(SchedulerDriver driver, FrameworkID frameworkId, MasterInfo masterInfo) {
        System.out.println("Registered with framework id " + frameworkId);

    }

    public void reregistered(SchedulerDriver driver, MasterInfo masterInfo) {
        // TODO Auto-generated method stub

    }

    public void resourceOffers(SchedulerDriver driver, List<Offer> offers) {
        synchronized (jobs) {
            List<Job> pendJobs = new ArrayList<Job>();
            for (Job job : jobs) {
                if (!job.isSubmitted()) {
                    pendJobs.add(job);
                }
            }

            for (Offer offer : offers) {
                if (pendJobs.isEmpty()) {
                    driver.declineOffer(offer.getId());
                    break;
                }

                Job job = pendJobs.remove(0);
                TaskInfo taskInfo = job.makeTask(offer.getSlaveId());
                driver.launchTasks(Collections.singletonList(offer.getId()), Collections.singleton(taskInfo));

                job.setSubmitted(true);
                System.out.println("Launched offer: " + taskInfo);
            }
        }
    }

    public List<TaskInfo> doFirstFit(Offer offer, List<Job> jobs) {
        List<TaskInfo> toLaunch = new ArrayList<TaskInfo>();
        List<Job> launchedJobs = new ArrayList<Job>();
        int offerCpus = 0;
        int offerMem = 0;
        // We always need to extract the resource info from the offer.
        // It's a bit annoying in every language.
        for (Resource r : offer.getResourcesList()) {
            if (r.getName().equals("cpus")) {
                offerCpus += r.getScalar().getValue();
            } else if (r.getName().equals("mem")) {
                offerMem += r.getScalar().getValue();
            }
        }
        // Now, we will pack jobs into the offer
        for (Job j : jobs) {
            double jobCpus = j.getCpus();
            double jobMem = j.getMem();
            if (jobCpus <= offerCpus && jobMem <= offerMem) {
                offerCpus -= jobCpus;
                offerMem -= jobMem;
                toLaunch.add(j.makeTask(offer.getSlaveId()));
                j.setSubmitted(true);
                launchedJobs.add(j);
            }
        }
        for (Job job : launchedJobs) {
            job.launch();
        }

        jobs.removeAll(launchedJobs);

        return toLaunch;
    }

    public void offerRescinded(SchedulerDriver driver, OfferID offerId) {
        // TODO Auto-generated method stub

    }

    public void statusUpdate(SchedulerDriver driver, TaskStatus status) {
        System.out.println("Got status update " + status);

        synchronized (jobs) {
            // We'll see if we can find a job this corresponds to
            for (Job job : jobs) {
                if (job.getId().equals(status.getTaskId().getValue())) {
                    switch (status.getState()) {
                        case TASK_RUNNING:
                            job.started();
                            break;
                        case TASK_FINISHED:
                            job.succeed();
                            break;
                        case TASK_FAILED:
                        case TASK_KILLED:
                        case TASK_LOST:
                        case TASK_ERROR:
                            job.fail();
                            break;
                        default:
                            break;
                    }
                }
            }
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
        // TODO Auto-generated method stub

    }

    public static void main(String[] args) throws Exception {
        byte[] data = Files.readAllBytes(Paths.get(args[1]));
        JSONObject config = new JSONObject(new String(data, "UTF-8"));
        JSONArray jobsArray = config.getJSONArray("jobs");
        List<Job> jobs = new ArrayList<Job>();
        for (int i = 0; i < jobsArray.length(); i++) {
            jobs.add(Job.fromJSON(jobsArray.getJSONObject(i)));
        }

        System.out.println(jobs);

        FrameworkInfo frameworkInfo = FrameworkInfo.newBuilder().setUser("").setName("Useless Remote BASH").build();
        Scheduler scheduler = new UselessRemoteBASH();

        SchedulerDriver driver = new MesosSchedulerDriver(scheduler, frameworkInfo, "zk://" + args[0] + "/mesos");

        driver.start();
        driver.stop();
    }

}
