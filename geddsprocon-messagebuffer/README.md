# geddsprocon-messagebuffer (Deprecated)
The message-buffer is a separate process and runs besides the data stream processing pipeline and is used to buffer all 
data which is sent/received by the pipeline. It is necessary to start the message-buffer before running the data stream 
processing pipeline. 

Use `mvn clean install` to compile an executable jar.

The message-buffer process can be started with the following command:

`java -jar ./target/geddsprocon-spark-examples-1.0-SNAPSHOT.jar 'ipc:///<message-buffer-connection-string>' 
<add-message-id-frame>`

 Parameter | Values |  Parameter description 
 ----- | --- | -------- 
`<message-buffer-connection-string>` | String | Can be an arbitrary string. Is used to initiate the message-buffer and determines at what address the buffer is reachable. |
`<add-message-id-frame>` | `["true", "false"]` | Adds a message-id frame as an additional frame to the messages. That frame is needed in case messages need to be re-sent. **Must** be set to **"true"** in combination with output operators and set to **"false"** in combination with input operators. 

