package org.example;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;

public class App {
    public static void main(String[] args) {
        String bucketName = "mtmv-bguproject";
        String jarPath = "s3://" + bucketName + "/DIRTProject/jar/DIRT-MapReduce-Assignment-1.0-SNAPSHOT.jar";

        //String inputPath = "s3://" + bucketName + "/DIRTProject/input/"; //for 10
        String inputPath =  "s3://" + bucketName + "/DIRTProject/input_100/"; //for 100 (99 last one is corrupted)
        String posPredsPath = "s3://" + bucketName + "/DIRTProject/positive-preds.txt";
        String negPredsPath = "s3://" + bucketName + "/DIRTProject/negative-preds.txt";
        String outputPath = "s3://" + bucketName + "/DIRTProject/output/run_" + System.currentTimeMillis();

        String accessKey = System.getenv("AWS_ACCESS_KEY_ID");
        String secretKey = System.getenv("AWS_SECRET_ACCESS_KEY");
        String sessionToken = System.getenv("AWS_SESSION_TOKEN");

        AWSCredentialsProvider credentialsProvider;
        if (sessionToken != null && !sessionToken.isEmpty()) {
            credentialsProvider = new AWSStaticCredentialsProvider(new BasicSessionCredentials(accessKey, secretKey, sessionToken));
        } else {
            credentialsProvider = new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey));
        }

        AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClientBuilder.standard()
                .withRegion("us-east-1")
                .withCredentials(credentialsProvider)
                .build();

        HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
                .withJar(jarPath)
                .withArgs(inputPath, posPredsPath, negPredsPath, outputPath);

        StepConfig stepConfig = new StepConfig()
                .withName("DIRT Discovery Step")
                .withHadoopJarStep(hadoopJarStep)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(3)
                .withMasterInstanceType(InstanceType.M5Xlarge.toString())
                .withSlaveInstanceType(InstanceType.M5Xlarge.toString())
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));

        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("DIRT MapReduce Job")
                .withReleaseLabel("emr-6.10.0")
                .withInstances(instances)
                .withSteps(stepConfig)
                .withLogUri("s3://" + bucketName + "/logs/")
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole");

        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Started DIRT Job with ID: " + jobFlowId);
    }
}