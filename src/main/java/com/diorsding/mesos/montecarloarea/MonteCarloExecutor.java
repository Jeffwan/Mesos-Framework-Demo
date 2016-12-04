package com.diorsding.mesos.montecarloarea;

import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.FrameworkInfo;
import org.apache.mesos.Protos.SlaveInfo;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;

public class MonteCarloExecutor implements Executor {

    Expression expression;
    double xLow;
    double xHigh;
    double yLow;
    double yHigh;
    int n;


    public MonteCarloExecutor(Expression expression, double xLow, double xHigh, double yLow, double yHigh, int n) {
        this.expression = expression;
        this.xLow = xLow;
        this.xHigh = xHigh;
        this.yLow = yLow;
        this.yHigh = yHigh;
        this.n = n;
    }

    public void registered(ExecutorDriver driver, ExecutorInfo executorInfo, FrameworkInfo frameworkInfo,
            SlaveInfo slaveInfo) {
        System.out.println("Registered an executor on slave " + slaveInfo.getHostname());

    }

    public void reregistered(ExecutorDriver driver, SlaveInfo slaveInfo) {
        // TODO Auto-generated method stub
        System.out.println("Re-Registered an executor on slave " + slaveInfo.getHostname());
    }

    public void disconnected(ExecutorDriver driver) {
        // TODO Auto-generated method stub
        System.out.println("Re-Disconnected the executor on slave");

    }

    public void launchTask(ExecutorDriver driver, TaskInfo task) {
        // TODO Auto-generated method stub

    }

    public void killTask(ExecutorDriver driver, TaskID taskId) {
        // TODO Auto-generated method stub
        System.out.println("Killing task " + taskId);
    }

    public void frameworkMessage(ExecutorDriver driver, byte[] data) {
        // TODO Auto-generated method stub

    }

    public void shutdown(ExecutorDriver driver) {
        // TODO Auto-generated method stub

        System.out.println("Shutting down the executor");
    }

    public void error(ExecutorDriver driver, String message) {
        // TODO Auto-generated method stub

    }

    public static void main(String[] args) {
        if (args.length < 6) {
            System.err.println("Usage: MonteCarloExecutor <Expression> <xLow> <xHigh> <yLow> <yHigh> <Number of Points>");
        }

        MesosExecutorDriver driver = new MesosExecutorDriver(new MonteCarloExecutor(Expression.fromString(args[0]),Double.parseDouble(args[1]), Double.parseDouble(args[2]), Double.parseDouble(args[3]), Double.parseDouble(args[4]), Integer.parseInt(args[5])));
        Protos.Status status = driver.run();
        System.out.println("Driver exited with status "+status);
    }

}