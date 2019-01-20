# geddsprocon-core
The core of **GeDDSProCon**. **Ge**o-**d**istributed **d**ata **s**tream **pro**cessor **con**nector allows user 
to connect geo-distributed heterogeneous data stream processors (Flink, Spark). As of now it is only possible to 
use the data stream processors Apache Spark and Apache Flink. 
# geddsprocon-evaluation
These classes are used for our evaluations (local and in the CLoud). Each DSP implements the 
reference pipelines (Ref-Pipeline_A, Ref-Pipeline_B) and distributed pipelines (Dis-Pipeline_A, Dis-Pipeline_B).
# geddsprocon-messagebuffer (Deprecated)
The message-buffer is a separate process and runs besides the data stream processing pipeline and is used to buffer all 
data which is sent/received by the pipeline.
# geddsprocon-examples
Consists of the word-count examples of the supporting data stream processors. Can be used to understand how  
**geddsprocon** can be used. 
