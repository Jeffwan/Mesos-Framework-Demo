package com.diorsding.mesos.rendler;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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


public class CrawlExecutor implements Executor {

    public void registered(ExecutorDriver driver, ExecutorInfo executorInfo, FrameworkInfo frameworkInfo,
            SlaveInfo slaveInfo) {
        System.out.println("Registered executor on " + slaveInfo.getHostname());
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
        byte[] message = new byte[0];

        try {
            // Parse the links from the url
            String urlData = getUrlSource(url);
            List<String> links = getLinks(urlData);
            links.add(0, url);
            // Write list of links to byte array
            String linkStr = "crawl" + links.toString();
            message = linkStr.getBytes();
        } catch (IOException e) {
            System.out.println("Link may not be valid.  Error parsing the html: " + e);
        }

        // Send framework message and mark the task as finished -- data communication.
        driver.sendFrameworkMessage(message);
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
        MesosExecutorDriver driver = new MesosExecutorDriver(new CrawlExecutor());
        System.exit(driver.run() == Status.DRIVER_STOPPED ? 0 : 1);
    }

    /**
     * Extract the links from a webpage.
     *
     * @param pHtml the html source
     * @return the list of links
     *
     **/


    /**
     * Extract the html source code from a webpage.
     *
     * @param pURL the page url
     * @return the source code
     *
     **/
    private String getUrlSource(String pURL) throws IOException {
        URL siteURL = new URL(pURL);
        URLConnection connection = siteURL.openConnection();
        BufferedReader myBufReader = new BufferedReader(new InputStreamReader(connection.getInputStream(), "UTF-8"));
        String inputLine;
        StringBuilder mySB = new StringBuilder();
        while ((inputLine = myBufReader.readLine()) != null)
            mySB.append(inputLine);
        myBufReader.close();

        return mySB.toString();
    }

    private List<String> getLinks(String pHtml) {
        Pattern linkPattern =
                Pattern.compile("<a[^>]+href=[\"']?([\"'>]+)[\"']?[^>]*>(.+?)</a>", Pattern.CASE_INSENSITIVE
                        | Pattern.DOTALL);
        Matcher pageMatcher = linkPattern.matcher(pHtml);
        ArrayList<String> links = new ArrayList<String>();
        while (pageMatcher.find()) {
            String fullLink = pageMatcher.group();
            int startQuoteIndex = fullLink.indexOf("\"");
            int endQuoteIndex = fullLink.indexOf("\"", startQuoteIndex + 1);
            String link = fullLink.substring(startQuoteIndex + 1, endQuoteIndex);
            // Heuristic used to check for valid urls
            if (link.contains("http") && !link.endsWith("signup")) {
                links.add(link);
            }
        }
        return links;
    }

}
