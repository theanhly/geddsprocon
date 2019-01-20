# geddsprocon-evaluation
These classes are used for our evaluations (local and in the CLoud). Each DSP implements the 
reference pipelines (Ref-Pipeline_A, Ref-Pipeline_B) and distributed pipelines (Dis-Pipeline_A, Dis-Pipeline_B).

##  firstpart
This package includes the first pipeline with a single time window of 5 seconds (_Ref-Pipeline_A_, _Dis-Pipeline_A_). 
Each DSP package (`flink`, `spark`) contains pipeline _Ref-Pipeline_A_ (CompletePipeline) and distributed pipline parts of 
_Dis-Pipeline_A_ (\*DSP\*Input, \*DSP\*Output) that are deployed to distributed clusters. 

## secondpart
This package includes the second pipeline with two time windows of 5 seconds and 30 seconds (_Ref-Pipeline_B_, _Dis-Pipeline_B_). 
Each DSP package (`flink`, `spark`) contains pipeline _Ref-Pipeline_B_ (CompletePipeline) and distributed pipline parts of 
_Dis-Pipeline_B_ (\*DSP\*Input, \*DSP\*Output) that are deployed to distributed clusters. 

## Deploying the jobs
Each class takes parameters as input to set the buffer (e.g., `--buffer 1000`), input operator fault tolerance
(e.g., `--fault-tolerance true`), and host (e.g., `--host localhost`), or port (e.g., `--port 5555`). Each DSP also includes 
a `StringSplitter` to stop the time and write the time in a log in folder `~/hadoop/thesis-evaluation/`. 
### Flink
```
// flink run
flink run -p 4 ./SparkOutput.jar --buffer 50000 --host localhost
```
### Spark
```
// spark-submit
spark-submit --deploy-mode client --master *MASTER* --num-executors 4 --executor-cores 4 --executor-memory 8g ./FlinkInput.jar  --buffer 50000 --host 172.31.44.42 
```