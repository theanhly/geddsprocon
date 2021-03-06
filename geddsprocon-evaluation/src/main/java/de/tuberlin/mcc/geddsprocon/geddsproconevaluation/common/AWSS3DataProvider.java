/*
 * Copyright 2019 The-Anh Ly
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package de.tuberlin.mcc.geddsprocon.geddsproconevaluation.common;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.common.SerializationTool;
import org.zeromq.ZMQ;

import java.io.*;

public class AWSS3DataProvider  implements Runnable {

    private String host;
    private int port;

    public AWSS3DataProvider(String host, int port) {
        this.host = host;
        this.port = port;
    }

    @Override
    public void run() {
        final String USAGE = "\n" +
                "To run this example, supply the name of an S3 bucket and object to\n" +
                "download from it.\n" +
                "\n" +
                "Ex: GetObject <bucketname> <filename>\n";

        String bucket_name = "tub-tal-master-thesis-bucket-output";
        String key_name = "amazon_reviews_us_Video_DVD_v1_00.tsv";
        String clientRegion = "eu-central-1";

        System.out.format("Downloading %s from S3 bucket %s...\n", key_name, bucket_name);
        final AmazonS3 s3 = AmazonS3ClientBuilder.standard()
                .withRegion(clientRegion)
                .withCredentials(new ProfileCredentialsProvider())
                .build();

        ZMQ.Context context = ZMQ.context(1);
        ZMQ.Socket sender = context.socket(ZMQ.PUSH);
        sender.setSndHWM(100000);
        sender.connect("tcp://" + this.host + ":" + this.port);
        try {
            S3Object o = s3.getObject(bucket_name, key_name);

            InputStreamReader inputReader = new InputStreamReader(o.getObjectContent());
            BufferedReader tsvReader = new BufferedReader(inputReader);

            // skip first line
            String newLine = tsvReader.readLine();
            newLine = tsvReader.readLine();
            sender.send(SerializationTool.serialize("START_DATA"));
            while(newLine != null && !newLine.isEmpty()) {
                String[] array = newLine.split("\t");
                sender.send(SerializationTool.serialize(array[13]));
                newLine = tsvReader.readLine();
            }
            System.out.println("ZeroMQDataProvider: Sending END_DATA" );
            sender.send(SerializationTool.serialize("END_DATA"));

            inputReader.close();
            o.close();
            tsvReader.close();
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            System.exit(1);
        } catch (FileNotFoundException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        } catch (IOException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
        System.out.println("Done!");
    }
}
