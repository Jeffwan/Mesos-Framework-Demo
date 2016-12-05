package com.diorsding.mesos.bash;

import java.io.UnsupportedEncodingException;
import java.util.UUID;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.mesos.Protos.CommandInfo;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.Value;
import org.json.JSONException;
import org.json.JSONObject;

public class Job {

    private String id;
    private double cpus;
    private double mem;
    private String command;
    private boolean submitted;
    private int retries;
    private JobState status;

    private Job() {
        submitted = false;
        id = UUID.randomUUID().toString();
        retries = 3;
    }

    public void launch() {
        status = JobState.STAGING;

        saveState();
    }

    public void started() {
        status = JobState.RUNNING;

        saveState();
    }

    public void succeed() {
        status = JobState.SUCCESSFUL;

        saveState();
    }

    public void fail() {
        if (retries == 0) {
            status = JobState.FAILED;
        } else {
            retries--;
            status = JobState.PENDING;
        }

        saveState();
    }

    private void saveState() {
        // TODO: This is Hack here. We should use global instance.
        CuratorFramework curator = CuratorFrameworkFactory.newClient("", new RetryOneTime(1000));
        JSONObject obj = new JSONObject();
        obj.put("id", id);
        obj.put("status", (status == JobState.STAGING ? JobState.RUNNING : status).toString());

        byte[] data = null;
        try {
            data = obj.toString().getBytes("UTF-8");
            curator.setData().forPath("/sampleframework/jobs" + id, data);
        } catch (UnsupportedEncodingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (Exception e) {
            try {
                curator.create().creatingParentsIfNeeded().forPath("/sampleframework/jobs" + id, data);
            } catch (Exception e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
        }
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public double getCpus() {
        return cpus;
    }

    public void setCpus(double cpus) {
        this.cpus = cpus;
    }

    public double getMem() {
        return mem;
    }

    public void setMem(double mem) {
        this.mem = mem;
    }

    public String getCommand() {
        return command;
    }

    public void setCommand(String command) {
        this.command = command;
    }

    public boolean isSubmitted() {
        return submitted;
    }

    public void setSubmitted(boolean submitted) {
        this.submitted = submitted;
    }

    public int getRetries() {
        return retries;
    }

    public void setRetries(int retries) {
        this.retries = retries;
    }

    public JobState getStatus() {
        return status;
    }

    public void setStatus(JobState status) {
        this.status = status;
    }

    public TaskInfo makeTask(SlaveID targetSlave) {
        UUID uuid = UUID.randomUUID();
        TaskID id = TaskID.newBuilder().setValue(uuid.toString()).build();
        return TaskInfo
                .newBuilder()
                .setName("task " + id.getValue())
                .setTaskId(id)
                .addResources(
                        Resource.newBuilder().setName("cpus").setType(Value.Type.SCALAR)
                                .setScalar(Value.Scalar.newBuilder().setValue(cpus)))
                .addResources(
                        Resource.newBuilder().setName("mem").setType(Value.Type.SCALAR)
                                .setScalar(Value.Scalar.newBuilder().setValue(mem))).setSlaveId(targetSlave)
                .setCommand(CommandInfo.newBuilder().setValue(command)).build();
    }

    public static Job fromJSON(JSONObject obj) throws JSONException {
        Job job = new Job();
        job.cpus = obj.getDouble("cpus");
        job.mem = obj.getDouble("mem");
        job.command = obj.getString("command");
        return job;
    }

    public static Job fromJSON(JSONObject jobJSON, CuratorFramework curator) {
        // TODO Auto-generated method stub
        return null;
    }
}
