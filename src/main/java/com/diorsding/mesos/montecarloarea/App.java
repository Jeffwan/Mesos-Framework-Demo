package com.diorsding.mesos.montecarloarea;

import java.util.Arrays;

import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;

public class App {

    public static void main(String[] args) {
        if (args.length < 8) {
            System.err
                    .println("Usage: MonteCarloScheduler <Master URI>  <Number of Tasks> <Curve Expression> <xLow> <xHigh> <yLow> <yHigh> <Number of Points>");
            System.exit(-1);
        }

        System.out.println("Starting the MonteCarloArea on Mesos with master " + args[0]);
        Protos.FrameworkInfo frameworkInfo =
                Protos.FrameworkInfo.newBuilder().setName("MonteCarloArea").setUser("Jiaxin").build();
        MesosSchedulerDriver schedulerDriver =
                new MesosSchedulerDriver(new MonteCarloScheduler(Arrays.copyOfRange(args, 2, args.length),
                        Integer.parseInt(args[1])), frameworkInfo, args[0]);
        schedulerDriver.run();
    }

}
