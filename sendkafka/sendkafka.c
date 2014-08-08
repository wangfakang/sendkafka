/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2012, Magnus Edenhill
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

/**
 * using the Kafka driver from librdkafka
 * (https://github.com/edenhill/librdkafka)
 */

#include <ctype.h>
#include <signal.h>
#include <string.h>
#include <fcntl.h>
#include <time.h>
#include <syslog.h>
#include <sys/stat.h>
#include<dirent.h>
/* Typical include path would be <librdkafka/rdkafkah>, but this program
 * is builtin from within the librdkafka source tree and thus differs. */
#include "librdkafka-0.7/rdkafka.h"  /* for Kafka driver */




/*
 *  declare function area
 * 
 */
char * getcurrenttime();
int read_config(const char * key, char * value, int size, const char * file);
void libwrite(const rd_kafka_t *rk, int level,const char *fac, const char *buf);
void sdkafkaerrloglocal(char *pathname,char*errinfo);
void controlquesizelog(rd_kafka_t **rks,int num,char *pathname);
int replysyslog(int facility,int level,char *markname,char *loginfo);
int write_log(int state,int level,char *info);
int getfilenum(char* pathname);
off_t  getfilesize(char *pathname);
void newname(char *pathname , int num);
int  logroate(char *pathname,int fd);
extern void rd_kafka_set_logger(void(*func)(const rd_kafka_t *rk,int level,const char *fac,const char *buf));
void productercircle(rd_kafka_t * *rks,char*topic,int partitions,int tag,char*opbuf,int len,int rkcount);
int  producter(rd_kafka_t **rks,char*topic,int partitions,int tag,char*buf,int len,int rkcount);
void kafkaqueuetof(rd_kafka_t**rks,int rkcount);
void savelocaldatatofile(char *opbuf);
static void stop (int sig);
void usage(const char * cmd);


/*
 *global_dflogpath means list or queue data save  path when main exit
 *global_efliblogpath means librdkafka err log path
 *global_efsdkfklogpath means sendkafka err log path
 *global_logwritelocal==0 default write log in file others in rsyslog
 *global_logmaxnum is means errlog max num
 *global_run is means run tages
 *global_logmaxsize is means errlog max size
 *monitortime is very 10 senconds will run mointorfunction
 */
static char global_dflogpath[1024]="/var/log/sendkafka/queue.dat";
static char global_efliblogpath[1024]="/var/log/sendkafka/error.log";
static char global_efsdkfklogpath[1024]="/var/log/sendkafka/errsdkafka.log";
static char global_monitorlogpath[1024]="/var/log/sendkafka/monitor.log";
static int global_logwritelocal=0;
static int global_logmaxnum=5;
static int global_run = 1;
static off_t global_logmaxsize=1024*1024;

#define monitortime  10



/*
 *function signal function,if signal ,it will
 *make global_run = 0 and while stop as will
 *as
 */
static void stop (int sig) {
	global_run = 0;
        printf("signal   fun   start...\n");
}



/*
 *function read usr configure file,example 
 *if broker = "test" then key is broker,value 
 *is "test" ,if read valid value will return not
 *zero else return zero 
 */
int read_config(const char * key, char * value, int size, const char * file)
{
	char  buf[1024] = { 0 };
	char * start = NULL;
	char * end = NULL;
	int  found = 0;
	FILE * fp = NULL;
	int keylen = strlen(key);

	// check parameters
	if(key==NULL || strlen(key)==0 || value==NULL ||  size<=0 || file==NULL || strlen(file)==0 )
	{
		char buf[50]="key,value,size,file is null or zero";
		perror(buf);
      		exit(1);

	}

	if (NULL != (fp = fopen(file, "r"))) {
		while (fgets(buf, sizeof(buf), fp)) {
			start = buf;
			while (*start == ' ' || *start == '\t') start++;
			if (*start == '#') continue;
			if (strlen(start) <= keylen) continue;
			if (strncmp(start, key, keylen)) continue;
			if (start[keylen] != ' ' && start[keylen] != '\t' && start[keylen] != '=')
				continue;
			start += keylen;
			while (*start == '=' || *start == ' ' || *start == '\t') start++;
			end = start;
			while (*end && *end != '#' && *end != '\r' && *end != '\n') end++;
			*end = '\0';
			strncpy(value, start, size);  
			value[size-1] = '\0';
			found = 1;
			break;
		}
		fclose(fp);
		fp = NULL;
	}

	if (found) {
		return strlen(value);
	} else {
		return 0;
	}
}

/*
 *function show some help info for usr when the 
 *usr not expertly
 *
 */
void usage(const char * cmd)
{
	fprintf(stderr, "Usage: %s [-h] | [-b <host1[:port1][,host2[:port2]...]>]\n"
			"\n"
			" Options:\n"
			"  -h                print this help message\n"
			"  -b <brokers>      Broker addresses (localhost:9092)\n"
			"  -c <config>       config file (/etc/sendkafka.conf)\n"
			"  -t <topic>        topic default rdfile (/etc/sendkafka.conf)\n"
			"  -l <global_efliblogpath> liberr path+name  (/etc/sendkafka.conf)\n"
			"  -s <global_efsdkfklogpath> sendkafkaerr path+name  (/etc/sendkafka.conf)\n"
			"  -d <global_dflogpath> queue data path+name  (/etc/sendkafka.conf)\n"
			"  -x <global_monitorlogpath> monitor queue size file path+name  (/etc/sendkafka.conf)\n"
                        "  -m <logmaxsize>      log max size default 124k (/etc/sendkafka.conf)\n"
			"  -n <global_logmaxnum>      log file num default 5 (/etc/sendkafka.conf)\n"
		        "  -o <global_logwritelocal>  global_logwritelocal default 0 ,if 0 write local other reresyslog(/etc/sendkafka.conf)\n"
			"\n"

			"  Config Format:\n"
			"  brokers = <host2[:port1][,host2[:port2]...]>\n"
			"  topic = <topic>\n"
			"  partitions = <partitions>\n"
			"  data_filelogpath = <daflogpath>   path+name example: /var/log/test.txt\n"
			"  err_filelibrdkafkalogpath = <erflogpath>   path+name example: /var/log/test.txt\n"
			"  err_filesendkafkalogpath = <err_fslogpath>   path+name example: /var/log/test.txt\n"
			"  global_logwritelocal = <global_logwritelocal>   default 0 means write log in local others rersyslog\n"
			"  global_logmaxnum = <global_logmaxnum>   default 5  , should 0--9\n"
			"\n",
		cmd);
	exit(1);
}



/*
 *function if success will return file size 
 *else return -1
 */
off_t  getfilesize(char *pathname)
{
    struct stat buff;
    if(0!=access(pathname,F_OK))
    {
	return -1;
    }
    if(0==stat(pathname,&buff))
    {

	return buff.st_size;
    }
    else
    {

        return -1;
    }
   
}


/*
 *function get pathname file  total num example
 *if  <pathname-0,pathname-1,pathname-2....> the
 *total num=3
 */
int getfilenum(char* pathname)
{
    int i=0;
    int num=0;
    char path[128]={0};
    strcpy(path,pathname);
    char c='0';
    strncat(path,"-",2);
    int len=strlen(path);
    printf("len : %d\n",len);
    for(;i<global_logmaxnum;++i,c=c+1)
    {
        path[len]=c;
        path[len+1]='\0';
	if(0 == access(path,F_OK))
	{
		++num;
	}
  
    }

    return num;

}

/*
 *function rename file new name for i to global_logmaxnum  
 *example  < name-0 --> name-1  name-1-->name-2...>
 *
 */
void newname(char *pathname , int num)
{
     int i=0;
     char buf[128]={0};
     char newbuf[128]={0};
     strcpy(buf,pathname);
     strncat(buf,"-",1);
     int len=strlen(buf);
    
     if(0==num)
     {
		strncat(buf,"0",1);
		rename(pathname,buf);
		return;
     }

     char c= ( (num==global_logmaxnum) ? (global_logmaxnum-2):(num)) + '0';
     memcpy(newbuf,buf,len+1);
     for(i=num-1;i>=0;--i,c=c-1)
     {
		buf[len]=c-1;
		buf[len+1]='\0';
		newbuf[len]=c;
        newbuf[len+1]='\0';
		rename(buf,newbuf);
     }

     memset(buf,'\0',128);
     memcpy(buf,pathname,len-1);
     strncat(buf,"-",1);
     strncat(buf,"0",1);
     rename(pathname,buf);

}



/*
 *function roate log depends on file's size ,if the 
 *file size more than maxsize ,the file will cut apart
 *and usr can configure the maxsize 
 */
int  logroate(char *pathname,int fd)
{
     if( getfilesize(pathname) >= global_logmaxsize )
     {
         char buf[128]={0};
         strcpy(buf,pathname);
		 int len=strlen(buf);	 
		 int num=getfilenum(pathname);

	 if(num == global_logmaxnum)
	 {
		
		strncat(buf,"-",2);
		buf[len+1]=global_logmaxnum + '0'-1;
		buf[len+2]='\0';
		unlink(buf);
	 }

        close(fd); 
       
        newname(pathname,num); 
	      

        int newfd=open(pathname,O_WRONLY|O_APPEND|O_CREAT,0666);

        if(fd == -1)
        {
            char buf[100]={0};
            sprintf(buf,"%d",__LINE__ -4);
            strncat(buf,"  line open global_efliblogpath  fail...",strlen("  line open global_efliblogpath  fail..."));
            write_log(global_logwritelocal,LOG_CRIT,buf);
            perror(buf);
            exit(1);
        }

        return newfd;

     }

    return -1;

}



/*
 *function  return  the  time since the Epoch
 *       (00:00:00 UTC, January 1, 1970), measured in sec-onds
 *
 */
time_t getcurrents()
{
    return time(NULL);
}
 


/*function get system time and return string
 *waring the string will store '\n'
 */
char * getcurrenttime()
{
     time_t t=time(NULL);

     return  asctime(localtime(&t)); 
}





/*
 *function monitor librdkafka queue and write to local  
 *file the path will depend on usr configure
 */
void controlquesizelog(rd_kafka_t **rks,int num,char *pathname)
{
	
	if((getcurrents() % monitortime) == 0)
	{
		char buf[128]={0};
		int i=0;
    		if(access(pathname,F_OK)!=0)
    		{
                     if(creat(pathname,0666) == -1)
                     {
                           char buf[100]={0};
                           sprintf(buf,"%d",__LINE__ -3);
                           strncat(buf,"  line pathname  error...",strlen("  line pathname  error..."));
                           write_log(global_logwritelocal,LOG_CRIT,buf);
                           perror(buf);
                           exit(1);
                    }
    		}

   		if(access(pathname,W_OK)!=0)
		{
			  char buf[100]={0};
			  sprintf(buf,"%d",__LINE__ -3);
			  strncat(buf,"  line open  pathname  no permit...",strlen("  line open  pathname  no permit..."));
			  write_log(global_logwritelocal,LOG_CRIT,buf);
			  perror(buf);
			  exit(1);
		}

		struct timeval tv;
		gettimeofday(&tv, NULL);
    	int fd=open(pathname,O_WRONLY|O_APPEND|O_CREAT,0666);
		char timebuf[50]={0};
		strncpy(timebuf,getcurrenttime(),strlen(getcurrenttime()));
	    timebuf[strlen(timebuf)-1]='\0';

		for(;i<num;++i)
		{
			sprintf(buf, "%s|%s| queue size= %d\n",
			timebuf,
			rks[i] ? rks[i]->rk_broker.name : "",rd_kafka_outq_len(rks[i]) );
			write(fd,buf,strlen(buf));
			memset(buf,'\0',128);
		}

		int newfd=logroate(pathname,fd);
		fd= newfd>0 ? newfd:fd;
		
        close(fd);
	}
}

/*
 *function write librdkafka log info to local
 *file the path will depend on usr configure
 */

void libwrite(const rd_kafka_t *rk, int level,const char *fac, const char *buf)
{
	struct timeval tv;
	gettimeofday(&tv, NULL);
        if(access(global_efliblogpath,F_OK)!=0)
        {
		
             if(creat(global_efliblogpath,0666) == -1)
                {
                        char buf[100]={0};
                        sprintf(buf,"%d",__LINE__ -3);
                        strcat(buf," line create pathname  error");
                        perror(buf);
                        exit(1);
                 }

        }
        if(access(global_efliblogpath,W_OK)!=0)
        {
             char buf[100]={0};
             sprintf(buf,"%d",__LINE__ -3);
             strncat(buf,"  line global_efliblogpath  error...",strlen("  line global_efliblogpath  error..."));
             perror(buf);
             exit(1);

        }

	int fd=open(global_efliblogpath,O_CREAT|O_WRONLY|O_APPEND,0666);

	if(fd == -1)
	{
		char buf[100]={0};
		sprintf(buf,"%d",__LINE__ -4);
		strncat(buf,"  line open global_efliblogpath  fail...",strlen("  line open global_efliblogpath  fail..."));
		perror(buf);
		exit(1);
	}

	char errbuf[1024]={0};
	sprintf(errbuf, "%%%i|%u.%03u|%s|%s| %s\n",
	level, (int)tv.tv_sec, (int)(tv.tv_usec / 1000),
	fac, rk ? rk->rk_broker.name : "", buf);
	
	int newfd=logroate(global_efliblogpath,fd);
	fd= newfd>0 ? newfd:fd;
        
	write(fd,errbuf,strlen(errbuf));

	close(fd);
}




/*
 *function write sendkafka log info to local
 *file the path will depend on usr configure
 */
void sdkafkaerrloglocal(char *pathname,char*errinfo)
{
   
     if(access(pathname,F_OK)!=0)
     {
         if(creat(pathname,0666) == -1)
         {
                char buf[100]={0};
                sprintf(buf,"%d",__LINE__ -3);
                strncat(buf,"  line pathname  error...",strlen("  line pathname  error..."));
                perror(buf);
                exit(1);
         }
    }

    if(access(pathname,W_OK)!=0)
    {
        char buf[100]={0};
        sprintf(buf,"%d",__LINE__ -3);
        strncat(buf,"  line pathname  no permit...",strlen("  line pathname  no permit..."));
        perror(buf);
        exit(1);
    }
    int fd=open(pathname,O_WRONLY|O_APPEND|O_CREAT,0666);

    if(fd == -1)
    {
        char buf[100]={0};
        sprintf(buf,"%d",__LINE__ -4);
        strncat(buf,"  line open pathname  fail...",strlen("  line open pathname  fail..."));
        perror(buf);
        exit(1);
    }

    if(NULL!=errinfo)
    {
        int newfd=logroate(pathname,fd);

        fd= newfd>0 ? newfd:fd;
        write(fd,errinfo,strlen(errinfo));
    }
   
    close(fd);

}


/*
 *function it will write some log info to rsyslog 
 *facility: log  type,level: log priority,markname 
 *is target indent, loginfo:log content
 */
int replysyslog(int facility,int level,char *markname,char *loginfo)
{

    openlog(markname,LOG_CONS|LOG_PID,facility);
    syslog(level,"sendkafka's  error reply  rsyslog :  %s ",loginfo);
    closelog();

    return 0;

}



/*
 *function: write log to file or rsyslog depend on state
 *if state is zero that will write log to usr configure path
 *else reply rsyslog 
 */

int write_log(int state,int level,char *info)
{
     char *perrbuf=NULL;
     if(NULL != info)
     {
         perrbuf=strdup(info);
     }
     if(state == 0)
     {
         rd_kafka_set_logger(libwrite);
         if(NULL!=info){
                sdkafkaerrloglocal(global_efsdkfklogpath,perrbuf);
         }
     }
     else
     {
        if(NULL!=info){
               replysyslog(LOG_LOCAL0,level,"SENDKAFKA: ",perrbuf);
          }


          rd_kafka_set_logger(rd_kafka_log_syslog);
     }

     free(perrbuf);
     perrbuf=NULL;
     return 0;
}



/*
 *function: check librdkafka queue and write it to  
 * local file if the queue not empty,the path will
 * depend on usr configure 
 */
void kafkaqueuetof(rd_kafka_t**rks,int rkcount)
{

   int fd=open(global_dflogpath,O_WRONLY|O_APPEND|O_CREAT,0666);

   if(fd == -1)
   {
        char buf[100]={0};
        sprintf(buf,"%d",__LINE__ -4);
        strncat(buf,"  line open global_dflogpath  fail...",strlen("  line open global_dflogpath  fail..."));
        write_log(global_logwritelocal,LOG_CRIT,buf);
        perror(buf);
        exit(1);
   }

   rd_kafka_op_t *rko=NULL;
   int i=0;
   for (i = 0; i < rkcount; i++) 
   {
       while (rd_kafka_outq_len(rks[i]) > 0)
       {
            rko = rd_kafka_q_read(&(rks[i]->rk_op), RD_POLL_INFINITE);
            write(fd,rko->rko_payload,rko->rko_len);         
       }
   }
   
    close(fd);

}

/*
 *function save opbuf to local file when error exit
 *and the file path depends usr configure
 */
void savelocaldatatofile(char *opbuf)
{
    if(opbuf==NULL || strlen(opbuf))return;
    if(getfilesize(global_dflogpath)>0)
    {
          unlink(global_dflogpath);
    }

     int fd=open(global_dflogpath,O_WRONLY|O_APPEND|O_CREAT,0666);

     if(fd == -1)
     {
        char buf[100]={0};
        sprintf(buf,"%d",__LINE__ -4);
        strncat(buf,"  line open global_dflogpath  fail...",strlen("  line open global_dflogpath  fail..."));
        write_log(global_logwritelocal,LOG_CRIT,buf);
        perror(buf);
        exit(1);
     }
     
     write(fd,opbuf,strlen(opbuf));
         
     close(fd);

}

/*
 *function get stdin or local file opbuf to librdkafka queue
 *circle  opbuf to queue when one fail,return zero of success 
 *if fail will return 1 
 */
int  producter(rd_kafka_t **rks,char*topic,int partitions,int tag,char*buf,int len,int rkcount)
{
    int i=0;
	int partition=0;
	int rk=0;
	int ret=0;
	rk=rand()%rkcount;

        for(;i<rkcount;++i,++rk)
        {
				rk%=rkcount;
				partition=rand()%partitions;
				ret = rd_kafka_produce(rks[rk],topic,partition,tag,buf,len);
				if(ret ==0 )
				{
					 return 0;
				}
				else
				{
					 char timebuf[50]={0};
					 strcpy(timebuf,getcurrenttime());
					 timebuf[strlen(timebuf)-1]='\0';
					 fprintf(stderr, "%s sendkafka[%d]: failed: %s\n",timebuf,getpid(), buf);
					 char *buf=calloc(1,strlen(buf)+128);
					 sprintf(buf,"%s sendkafka[%d]: failed: %s\n",timebuf,getpid(), buf);
					 write_log(global_logwritelocal,LOG_INFO,buf);
					 free(buf);
					 buf=NULL;
					 continue;
				}
        }

        return 1;

}


/*
 *function circle send opbuf to librdkafka queue ,
 *if the five time all failed and will exit , 
 *will write some error to local file ,and check
 *librdkafka queue data write file if size not zero
 *
 */
void productercircle(rd_kafka_t * *rks,char*topic,int partitions,int tag,char*opbuf,int len,int rkcount)
{
     int failnum=0;
     int s=1;
     while(s)
     {
           s=producter(rks,topic,partitions,RD_KAFKA_OP_F_FREE,opbuf,len,rkcount);
           controlquesizelog(rks,rkcount,global_monitorlogpath);
           if(s==1){
                sleep(1);
                if(++failnum == 5){
                        char timebuf[50]={0};
                        strcpy(timebuf,getcurrenttime());
                        timebuf[strlen(timebuf)-1]='\0';
                        fprintf(stderr, "%s all broker down \n",timebuf);
                        char *buf=calloc(1,strlen(opbuf)+128);
                        sprintf(buf,"%s all broker down \n",timebuf);
                        write_log(global_logwritelocal,LOG_INFO,buf);
                        free(buf);
                        buf=NULL;
						savelocaldatatofile(opbuf);
                        kafkaqueuetof(rks,rkcount);
                        exit(1);
                }
            }
     }
}




int main (int argc, char **argv)
{
	rd_kafka_t *rks[1024] = { 0 };
	int  	    rkcount = 0;
	char 	    value[1024] = { 0 };
	char 	    brokers[1024] = "localhost:9092";
	char 	    *broker = NULL;
	char 	    topic[1024] = "topic";
	int   	    sendcnt=0;
	int  	    partitions = 4;
	int  	    opt;
	int  	    len=0;
    char 	    *opbuf=NULL;

        
	if (read_config("brokers", value, sizeof(value), "/etc/sendkafka.conf") > 0) {
		strcpy(brokers, value);
                memset(value,'\0',1024);
	}
	if (read_config("topic", value, sizeof(value), "/etc/sendkafka.conf") > 0) {
		strcpy(topic, value);
                memset(value,'\0',1024);
	}
	if (read_config("partitions", value, sizeof(value), "/etc/sendkafka.conf") > 0) {
		partitions = atoi(value);
		if (partitions <= 0 || partitions > 256) {
			partitions = 4;
		}
	}
  
        if (read_config("data_filelogpath", value, sizeof(value), "/etc/sendkafka.conf") > 0) {
		strcpy(global_dflogpath, value);
		memset(value,'\0',1024);
	}
	if (read_config("err_filelibrdkafkalogpath", value, sizeof(value), "/etc/sendkafka.conf") > 0) {
		strcpy(global_efliblogpath, value);
		memset(value,'\0',1024);
	}
	if (read_config("err_filesendkafkalogpath", value, sizeof(value), "/etc/sendkafka.conf") > 0) {
		strcpy(global_efsdkfklogpath, value);
		 memset(value,'\0',1024);
	}

	if (read_config("global_logwritelocal", value, sizeof(value), "/etc/sendkafka.conf") > 0) {
		global_logwritelocal=atoi(value);
	}
	if (read_config("global_logmaxnum", value, sizeof(value), "/etc/sendkafka.conf") > 0) {
		global_logmaxnum=atoi(value);
	}

	if (read_config("global_logmaxsize", value, sizeof(value), "/etc/sendkafka.conf") > 0) {
		global_logmaxsize=atoi(value);
	}

	if (read_config("global_monitorlogpath", value, sizeof(value), "/etc/sendkafka.conf") > 0) {
		strcpy(global_monitorlogpath, value);
		memset(value,'\0',1024);
	}
	while ((opt = getopt(argc, argv, "hb:c:d:p:t:o:m:n:l:s:x:")) != -1) {
		switch (opt) {
		case 'b':
			strncpy(brokers, optarg, sizeof(brokers));
			brokers[sizeof(brokers)-1] = '\0';  
			break;
		case 'c':
			if (read_config("brokers", value, sizeof(value), optarg) > 0) {
				strcpy(brokers, value);
                                memset(value,'\0',1024);
			}
                  
			if (read_config("topic", value, sizeof(value), optarg) > 0) {
				strcpy(topic, value);
                                memset(value,'\0',1024);
			}

			if (read_config("partitions", value, sizeof(value), optarg) > 0) {
				partitions=atoi(value);
                                memset(value,'\0',1024);
			}
                        
			if (read_config("data_filelogpath", value, sizeof(value), optarg) > 0) {
				
				strcpy(global_dflogpath, value);
                                memset(value,'\0',1024);
			}
			if (read_config("monitor_logpath", value, sizeof(value), optarg) > 0) {
				
				strcpy(global_monitorlogpath, value);
                                memset(value,'\0',1024);
			}
			if (read_config("err_filelibrdkafkalogpath", value, sizeof(value), optarg) > 0) {
				
				strcpy(global_efliblogpath, value);
                                memset(value,'\0',1024);
			}
			if (read_config("err_filesendkafkalogpath", value, sizeof(value), optarg) > 0) {
				
				strcpy(global_efsdkfklogpath, value);
                                memset(value,'\0',1024);
			}
                        
			if (read_config("global_logwritelocal", value, sizeof(value), optarg) > 0) {
				
                                global_logwritelocal=atoi(value);
			}
			if (read_config("global_logmaxnum", value, sizeof(value), optarg) > 0) {
				
                                global_logmaxnum=atoi(value);
			}
			if (read_config("global_logmaxsize", value, sizeof(value), optarg) > 0) {
				
                                global_logmaxsize=atoi(value);
			}
			break;

                case 'o':
                        if(NULL!=optarg){
                                global_logwritelocal=atoi(optarg);
			}
			break;    
		case 't':
			if(NULL!=optarg){
				strncpy(topic, optarg, sizeof(topic));
                                topic[sizeof(topic)-1] = '\0';
			}
			break;
		case 'p':
			if(NULL!=optarg){
				partitions=atoi(optarg);
			}
			break;
		case 'm':
			if(NULL!=optarg){
				global_logmaxsize=atoi(optarg);
			}
			break;
		case 'l':
			if(NULL!=optarg){

				memset(global_efliblogpath,'\0',strlen(global_efliblogpath));
				strcpy(global_efliblogpath, optarg);
			}
			break;

		case 's':
			if(NULL!=optarg){
				memset(global_efsdkfklogpath,'\0',strlen(global_efsdkfklogpath));
				strcpy(global_efsdkfklogpath, optarg);
			}
			break;
		case 'd':
			if(NULL!=optarg){
				memset(global_dflogpath,'\0',strlen(global_dflogpath));
				strcpy(global_dflogpath, optarg);
			}
			break;
		case 'x':
			if(NULL!=optarg){
				memset(global_monitorlogpath,'\0',strlen(global_monitorlogpath));
				strcpy(global_monitorlogpath, optarg);
			}
			break;
		case 'n':
			if(NULL!=optarg){
				global_logmaxnum=atoi(optarg);
			}
			break;
		case 'h':
		default:
			usage(argv[0]);
			break;
		}
	}
        
        write_log(global_logwritelocal,0,NULL);

	signal(SIGINT, stop);
	signal(SIGTERM, stop);
	// see: https://github.com/edenhill/librdkafka/issues/2
	signal(SIGPIPE, SIG_IGN);
	signal(SIGHUP,stop); 
       /* Producer
	 */
	char buf[4096];
	//int sendcnt = 0;
	int i=0;
	/* Create Kafka handle */
	for (broker = strtok(brokers, ","), rkcount = 0;
	     broker && rkcount < sizeof(rks);
	     broker = strtok(NULL, ","), ++rkcount) 
	{
		rks[rkcount] = rd_kafka_new(RD_KAFKA_PRODUCER, broker, NULL);
		if (!rks[rkcount]) {
			for (i = 0; i < rkcount; i++) {
				rd_kafka_destroy(rks[i]);
				rks[i] = NULL;
			}

            strcpy(buf,getcurrenttime());
			buf[strlen(buf)-1]='\0';
            strncat(buf,"kafka_new producer is fail...",29);                   
			perror(buf);
            write_log(global_logwritelocal,LOG_CRIT,buf);    
			global_run=0;

		}

	}

	srand(time(NULL));
	char *eptr=NULL;
    int state=0;
	while ( global_run ) 
	{
		if(state)
		{
			eptr=fgets(buf, sizeof(buf), stdin);
			if(EINTR==errno || NULL==eptr ){
		        	global_run=0;
			        break;
			}
		    ++sendcnt;	
		   	opbuf=strdup(buf);
			len=strlen(opbuf);
			productercircle(rks,topic,partitions,RD_KAFKA_OP_F_FREE,opbuf,len,rkcount);
			
		}
        else
		{
			FILE *fp=NULL;
			opbuf=NULL;
			if(access(global_dflogpath,F_OK)==0)
			{
		          fp=fopen(global_dflogpath,"r");
			      if(fp==NULL)
			      {
					 char buf[100]={0};
        			 sprintf(buf,"%d",__LINE__ -4);
        			 strncat(buf,"  line open global_dflogpath  fail...",strlen("  line open global_dflogpath  fail..."));
        			 write_log(global_logwritelocal,LOG_CRIT,buf);
				     perror(buf);
				  
					 exit(1);
  			      }
			      while(fgets(buf,sizeof(buf),fp))
			      {
					   ++sendcnt;
					   opbuf=strdup(buf);
					   len=strlen(opbuf);
					   productercircle(rks,topic,partitions,RD_KAFKA_OP_F_FREE,opbuf,len,rkcount);
			      }
			
   			     if(getfilesize(global_dflogpath)>0)
   			     {
         			   unlink(global_dflogpath);
  			     }
		         }
				 
				 state=1;
		         continue;
        	}

                if ((sendcnt % 100000) == 0 ) {

                     char timebuf[50]={0};
                     strcpy(timebuf,getcurrenttime());
                     timebuf[strlen(timebuf)-1]='\0';
                     fprintf(stderr, "%s sendkafka[%d]: Sent %i messages to topic %s\n", timebuf,getpid(), sendcnt, topic);

                     char *buf=calloc(1,strlen(topic)+128);
                     sprintf(buf,"%s sendkafka[%d]: Sent %i messages to topic %s\n",timebuf,getpid(), sendcnt, topic);
                     write_log(global_logwritelocal,LOG_INFO,buf);    
                     free(buf);
                     buf=NULL;
                }
			
		
      }


        printf("sendcnt num %d\n",sendcnt);
        kafkaqueuetof(rks,rkcount);
        
	/* Destroy the handle */
	for (i = 0; i < rkcount; i ++) {
		rd_kafka_destroy(rks[i]);
	}

	return 0;

}

