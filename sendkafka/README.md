#Pre-requisites

* sudo apt-get install zlib1g-dev
* need librdkafka installed in parent directory
 
* https://github.com/edenhill/librdkafka

* need kafka broker running locally



#Building and installing sendkafka



make

make install

* if you want to use the default configuration file, you need to perform the following two steps

make dir

make mv




#Sendkafka configuration
 

* All of the following to configuration parameters and the value must be between the characters '' or '=' or '\t'spaced ,'#' said notes a whole line.

* Warning when there are multiple processes running program ,all of its configuration file must be named after their program name and add ".conf" end(at rsyslog v7)

* All log files are default stored in the '/var/log/sendkafka/ ' directory below ,default error.log queue.data queuesize.log , if you want to use default you should be mkddir /var/log/sendkafka directory,and Need to move the sendkafka.conf configuration files to the /etc/ directory




```


* error.log save librdkafka or sendkafka error information

* queue.data save librdkafka queue or local data when program exit if it not empty 

* queuesize.log save program during the operation of librdkafka internal queue size

```



* topic  Topic to consume it defaults to "topic" (must less than 1024 Bytes).

topic = app_weiboplatformmz4oexg_haproxy


* partitions  partition to consume (value is an integer),it defaults to 4.

partitions = 4


* savelocal_tag if savelocal_tag=0 will write log to local file ,others write to syslog.

savelocal_tag = 0


* data_path is means librdkafka queue data file  path,to save queue data when main exitif it not empty (the path must less than 1024 Bytes).

data_path = /var/log/sendkafka/queue.data


* error_path is means librdkafka or sendkafka error log file path(the path must less than 1024 Bytes).

error_path = /var/log/sendkafka/errlog.log


* queue_sizepath is means check queue size file  path(the path must less than 1024 Bytes).
queue_sizepath = /var/log/sendkafka/queuesize.log


* monitor_period is default  very 10 senconds will run mointorfunction(every 10 seconds to check a queue size).
monitor_period = 10


* lognum_max is means errlog file max num (lognum_max value must be between 0 to 9 ).

lognum_max = 5


* logsize_max is means one errlog file max size (waring value must is an integer max 2^32 - 1 , max is 4G).do not allow the expression it default 1M

logsize_max = 1000000



#warning

* when there are multiple processes running program ,all of its configuration file must be named after their program name and add ".conf" end(at rsyslog v7)





