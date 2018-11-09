flume-ng agent --conf ../conf/ --conf-file flume.nf --name agent1
spark-submit --class com.dbocharov.detect.jobs.dstream.DetectBotJob target/scala-2.11/spark-detect-bot-project-assembly-0.1.jar
spark-submit --class com.dbocharov.detect.jobs.dstream.DetectBotJob target/scala-2.11/spark-detect-bot-project-assembly-0.1.jar