package com.diorsding.mesos.rendler;

import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos.CommandInfo;
import org.apache.mesos.Protos.Credential;
import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.FrameworkInfo;
import org.apache.mesos.Protos.Status;
import org.apache.mesos.Scheduler;

import com.google.protobuf.ByteString;

public class RendlerApp {

    public static void main(String[] args) {
        if (args.length < 1 || args.length > 3) {
            usage();
            System.exit(1);
        }

        String path = System.getProperty("user.dir") + "/target/rendler.jar";
        path = "/vagrant/mesos-framework-demo.jar";
        CommandInfo.URI uri = CommandInfo.URI.newBuilder().setValue(path).setExtract(false).build();

        ExecutorInfo executorInfoCrawl = getExecutorInfoCrawl(uri, path);
        ExecutorInfo executorInfoRender = getExecutorInfoRender(uri, path);

        FrameworkInfo.Builder frameworkBuilder =
                FrameworkInfo.newBuilder().setFailoverTimeout(120000).setUser("").setName("Rendler Framework (Java)");

        if (System.getenv("MESOS_CHECKPOINT") != null) {
            System.out.println("Enabling checkout for the framework");
            frameworkBuilder.setCheckpoint(true);
        }

        Scheduler scheduler =
                args.length == 1 ? new RendlerScheduler(executorInfoCrawl, executorInfoRender) : new RendlerScheduler(
                        executorInfoCrawl, executorInfoRender, Integer.parseInt(args[1]), args[2]);


        MesosSchedulerDriver driver = null;
        if (System.getenv("MESOS_AUTHENTICATE") != null) {
            System.out.println("Enabling authentication for the framework");

            if (System.getenv("DEFAULT_PRINCIPAL") == null) {
                System.err.println("Expecting authentication principal in the environment");
                System.exit(1);
            }

            if (System.getenv("DEFAULT_SECRET") == null) {
                System.err.println("Expecting authentication secret in the environment");
                System.exit(1);
            }

            Credential credential =
                    Credential.newBuilder().setPrincipal(System.getenv("DEFAULT_PRINCIPAL"))
                            .setSecretBytes(ByteString.copyFrom(System.getenv("DEFAULT_SECRET").getBytes())).build();

            frameworkBuilder.setPrincipal(System.getenv("DEFAULT_PRINCIPAL"));

            driver = new MesosSchedulerDriver(scheduler, frameworkBuilder.build(), args[0], credential);

        } else {
            // frameworkBuilder.setPrincipal("test-framework-java");

            driver = new MesosSchedulerDriver(scheduler, frameworkBuilder.build(), args[0]);
        }

        int status = driver.run() == Status.DRIVER_STOPPED ? 0 : 1;

        // Ensure that the driver process terminates.
        driver.stop();
        System.exit(status);

    }

    private static ExecutorInfo getExecutorInfoRender(CommandInfo.URI uri, String path) {
        String commandRender =
                "java -cp " + path + " com.diorsding.mesos.rendler.RenderExecutor";
        CommandInfo commandInfoRender = CommandInfo.newBuilder().setValue(commandRender).addUris(uri).build();

        ExecutorInfo executorInfoRender =
                ExecutorInfo.newBuilder().setExecutorId(ExecutorID.newBuilder().setValue("RenderExecutor"))
                        .setCommand(commandInfoRender)
                        .setData(ByteString.copyFromUtf8(System.getProperty("user.dir")))
                        .setName("Rendler Executor (Java)")
                        .setSource("java")
                        .build();
        return executorInfoRender;
    }

    private static ExecutorInfo getExecutorInfoCrawl(CommandInfo.URI uri, String path) {
        String commandCrawler =
                "java -cp " + path + " com.diorsding.mesos.rendler.CrawlExecutor";
        CommandInfo commandInfoCrawler = CommandInfo.newBuilder().setValue(commandCrawler).addUris(uri).build();

        ExecutorInfo executorInfoCrawl =
                ExecutorInfo.newBuilder().setExecutorId(ExecutorID.newBuilder().setValue("CrawlExecutor"))
                        .setCommand(commandInfoCrawler)
                        .setName("Crawl Executor (Java)")
                        .setSource("java")
                        .build();
        return executorInfoCrawl;
    }

    private static void usage() {
        String name = RendlerScheduler.class.getName();
        System.err.println("Usage: " + name + " master <tasks> <url>");
    }

}
