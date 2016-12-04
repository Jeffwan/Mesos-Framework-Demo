package com.diorsding.mesos.pi;


import org.apache.log4j.Logger;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos.CommandInfo;
import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.FrameworkInfo;
import org.apache.mesos.Protos.Status;
import org.apache.mesos.Scheduler;


/**
 * java -cp mesos-framework-demo.jar com.diorsding.mesos.pi.PiDriver "zk://localhost:2181/mesos"
 *
 * @author jiashan
 *
 */
public class PiDriver {

    private final static Logger logger = Logger.getLogger(PiDriver.class);

    public static void main(String[] args) {
        String path = System.getProperty("user.dir") + "/target/scala-2.10/mesos-pi-assembly-1.0.jar";

        // TODO: get ride of hardcoding here.
        path = "/vagrant/mesos-framework-demo.jar";
        /* Defining the executor */

        CommandInfo.URI uri = CommandInfo.URI.newBuilder().setValue(path).setExtract(false).build();
        String commandPi = "java -cp mesos-framework-demo.jar com.diorsding.mesos.pi.PiExecutor";
        CommandInfo piCommandInfo = CommandInfo.newBuilder().setValue(commandPi).addUris(uri).build();


        /* Setting the executor information */

        ExecutorInfo piExecutorInfo =
                ExecutorInfo.newBuilder().setExecutorId(ExecutorID.newBuilder().setValue("CalculatePi"))
                        .setCommand(piCommandInfo).setName("PiExecutor").setSource("java").build();

        /* Defining framework & specifying related information */

        FrameworkInfo.Builder frameworkBuilder =
                FrameworkInfo.newBuilder().setFailoverTimeout(120000).setUser("").setName("PiFramework")
                        .setPrincipal("test-framework-java");

        /* Enabling checkpointing */

        if (System.getenv("MESOS_CHECKPOINT") != null) {
            System.out.println("Enabling checkpoint for the framework");
            frameworkBuilder.setCheckpoint(true);
        }

        /* Initializing the scheduler */

        Scheduler scheduler = new PiScheduler(piExecutorInfo, 1);

        /* Defining the scheduler driver */

        MesosSchedulerDriver schedulerDriver = new MesosSchedulerDriver(scheduler, frameworkBuilder.build(), args[0]);;


        int status = schedulerDriver.run() == Status.DRIVER_STOPPED ? 0 : 1;
        schedulerDriver.stop();
        System.exit(status);
    }

}
