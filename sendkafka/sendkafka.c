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


static int run = 1;
static off_t logmaxsize=1024*1024;


static void stop (int sig) {
	run = 0;
        printf("signal   fun   start...\n");
}

int read_config(const char * key, char * value, int size, const char * file)
{
	char  buf[1024] = { 0 };
	char * start = NULL;
	char * end = NULL;
	int  found = 0;
	FILE * fp = NULL;
	int keylen = strlen(key);

	// check parameters
	assert(key  !=  NULL  &&  strlen(key)); 
	assert(value  !=  NULL);
	assert(size  >   0 );
	assert(file  != NULL  && strlen(file));


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

void usage(const char * cmd)
{
	fprintf(stderr, "Usage: %s [-h] | [-b <host1[:port1][,host2[:port2]...]>]\n"
			"\n"
			" Options:\n"
			"  -h                print this help message\n"
			"  -b <brokers>      Broker addresses (localhost:9092)\n"
			"  -c <config>       config file (/etc/sendkafka.conf)\n"
			"  -t <topic>        topic default rdfile (/etc/sendkafka.conf)\n"
			"  -l <global_eflogpath> liberr path+name  (/etc/sendkafka.conf)\n"
			"  -s <global_efslogpath> sendkafkaerr path+name  (/etc/sendkafka.conf)\n"
			"  -d <global_dflogpath> list or queue data path+name  (/etc/sendkafka.conf)\n"
			"  -m <listmaxsize>      list max size default 1M (/etc/sendkafka.conf)\n"
			"  -n <logmaxnum>      log file num default 5 (/etc/sendkafka.conf)\n"
			"  -o <logwriteinf>  logwriteinf default 0 ,if 0 write local other reresyslog(/etc/sendkafka.conf)\n"
			"\n"
			"  Config Format:\n"
			"  brokers = <host2[:port1][,host2[:port2]...]>\n"
			"  topic = <topic>\n"
			"  partitions = <partitions>\n"
			"  daflogpath = <daflogpath>   path+name example: /var/log/test.txt\n"
			"  erflogpath = <erflogpath>   path+name example: /var/log/test.txt\n"
			"  erfslogpath = <erfslogpath>   path+name example: /var/log/test.txt\n"
			"  logwrite = <logwrite>   default 0 means write log in local others rersyslog\n"
			"  logmaxnum = <logmaxnum>   default 5  , should 0--9\n"
			"\n",
		cmd);
	exit(1);
}

/*
 *function return file size at success else retun -1
 *
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
 *function get pathname's path pos 
 *
 */
int getpathpos(char*pathname)
{
   if(NULL==pathname)return -1;
   int pos=-1;
   int i=0;
   while(pathname[i]!='\0')
   {
      if(pathname[i]== '\\')
      {
   	  pos=i;
      }

   }


   return pos;

}

static int logmaxnum=5;

/*
 *function get pathname num all <pathname-0,pathname-1,pathname-2....>
 *
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
    for(;i<logmaxnum;++i,c=c+1)
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
 *function rename for i to logmaxnum <name-0 --> name-1  name-1-->name-2...>
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

     char c= ( (num==logmaxnum) ? (logmaxnum-2):(num)) + '0';
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
 *function roate log depends on file's size
 *
 */
int  logroate(char *pathname,int fd)
{
     if( getfilesize(pathname) >= logmaxsize )
     {
         char buf[128]={0};
         strcpy(buf,pathname);
	 int len=strlen(buf);	 
	 int num=getfilenum(pathname);

	 if(num == logmaxnum)
	 {
		
		strncat(buf,"-",2);
                buf[len+1]=logmaxnum + '0'-1;
                buf[len+2]='\0';
		unlink(buf);
	 }

        close(fd); 
       
        newname(pathname,num); 
	      

        int newfd=open(pathname,O_WRONLY|O_APPEND|O_CREAT,0666);
        assert(-1!=newfd);
        return newfd;

     }

    return -1;

}


/*
 *logwriteinf==0 default write log in file others in rsyslog
 */
static int logwriteinf=0;

/*
 *listmaxsize is list max size 
 */
static int listmaxsize=1024*1024;

/*
 *global_dflogpath means list or queue data save  path when main exit
 *global_eflogpath means librdkafka err log path
 *global_efslogpath means sendkafka err log path
 */
static char global_dflogpath[128]="/var/log/qorldlog.txt";
static char global_eflogpath[128]="/var/log/errlog.txt";
static char global_efslogpath[128]="/var/log/serrlog.txt";


/*function 把标准错误流里面的内容重定向到global_eflogpath中
 *主要是用于把lib中的err写入本地所配置的文件
 *
 * 目前未使用此函数
 */
int liberrloglocal(int f)
{
     int fd=open(global_eflogpath,O_RDWR|O_APPEND|O_CREAT,0666);
     assert(-1 != fd);
     close(f);
     if(dup(fd) == -1)
     {
	perror("dup is faill...");
     }
		
     return fd;
}
/*
 *function record librdkafka err at global_eflogpath
 *
 */

void libwrite(const rd_kafka_t *rk, int level,const char *fac, const char *buf)
{
	struct timeval tv;
	gettimeofday(&tv, NULL);
        if(access(global_eflogpath,F_OK)!=0)
        {
	     creat(global_eflogpath,0666);
        }
        if(access(global_eflogpath,W_OK)!=0)
        {
	     perror("open global_eflogpath");
	     exit(1);
        }
	int fd=open(global_eflogpath,O_CREAT|O_WRONLY|O_APPEND,0666);
	assert(-1!=fd);
	char errbuf[1024]={0};
	sprintf(errbuf, "%%%i|%u.%03u|%s|%s| %s\n",
	level, (int)tv.tv_sec, (int)(tv.tv_usec / 1000),
	fac, rk ? rk->rk_broker.name : "", buf);
	
        int newfd=logroate(global_eflogpath,fd);
        fd= newfd>0 ? newfd:fd;
        
	write(fd,errbuf,strlen(errbuf));

	close(fd);
}




/*
 *function 主要是把sendkafka中的err写入本地文件中
 *
 */
void skafkaerrloglocal(char *pathname,char*errinfo)
{
   
    if(access(pathname,F_OK)!=0)
    {
         creat(pathname,0666);
    }
    if(access(pathname,W_OK)!=0)
    {
   	 perror("open pathname");
         exit(1);
    }

    int fd=open(pathname,O_WRONLY|O_APPEND|O_CREAT,0666);
    assert(-1!=fd);
    if(NULL!=errinfo)
    {
        int newfd=logroate(pathname,fd);

        fd= newfd>0 ? newfd:fd;
        write(fd,errinfo,strlen(errinfo));
    }
   
    close(fd);

}
/*function 获取到当前系统的时间 用于记录日志时间
 *
 */
char * getcurrenttime1()
{
     time_t t=time(NULL);

     return  asctime(localtime(&t)); 
}

/* function 按照参数的指定把日志信息回写到rsyslog中
 * parameter  means：
 * facility 表示日志信息的来源 可在rsyslog.conf中进行配置
 * level    表示日志的级别
 * markname 表示在写日志的每一行上加上的标记信息
 * loginfo  表示要记录的日志信息
 */
int replysyslog(int facility,int level,char *markname,char *loginfo)
{

    openlog(markname,LOG_CONS|LOG_PID,facility);
    syslog(level,"sendkafka's  error reply  rsyslog :  %s ",loginfo);
    closelog();

    return 0;

}

/*
 *librdkafka function using it to set write log ways
 *
void rd_kafka_log_syslog (const rd_kafka_t *rk, int level,
			  const char *fac, const char *buf) {
	static int initialized = 0;

	if (!initialized)
		openlog("rdkafka", LOG_PID|LOG_CONS, LOG_USER);

	syslog(level, "%s: %s: %s", fac, rk ? rk->rk_broker.name : "", buf);
}
 */

extern void rd_kafka_set_logger(void(*func)(const rd_kafka_t *rk,int level,const char *fac,const char *buf));



/*
 * 下面是单链表用于保存从stdin中读取的数据
 * global_listsize表示链表目前的大小
 */
int global_listsize=0;

typedef struct node
{
     char  *pdata;
     struct node  *next; 
}nodeinfo; 

/*
 *function: write log to file or rsyslog depend on state
 *
 */

int write_log(int state,int level,char *info)
{
     char buf[128]={0};
     sprintf(buf,"%d",__LINE__);
     if(NULL != info)
     {
         strncat(buf,info,strlen(info));
     }
     if(state == 0)
     {   
         rd_kafka_set_logger(libwrite);
         if(NULL!=info){
                skafkaerrloglocal(global_efslogpath,buf);
	 }
     }
     else
     { 
        if(NULL!=info){
               replysyslog(LOG_LOCAL0,level,"SENDKAFKA: ",buf);
	  }


          rd_kafka_set_logger(rd_kafka_log_syslog);
     }

     return 0;
}


/*
 *function: inint list head
 *
 *
 */
void listinit(nodeinfo **phead)
{
   *phead = (nodeinfo*)malloc(sizeof(nodeinfo));
   if(*phead==NULL)
   {
      write_log(logwriteinf,LOG_CRIT,"malloc  faill...");    
   }
   (*phead)->next=NULL;
   (*phead)->pdata=NULL;
}


/*
 *function: delete node at the head back
 *
 *
 */
void delfirstnode(nodeinfo *head)
{
     assert(NULL!=head);
     nodeinfo*p=head->next;
     if(NULL!=p)
     {
         head->next=p->next;
     }
     global_listsize-=(strlen(p->pdata)+1);
     free(p);
     p=NULL;
}


/*
 *function: get node at the head back
 *
 *
 */
char *getfirstdata(nodeinfo *head)
{
   if(NULL==head  || NULL==head->next)
   {
       return NULL;
   }

   return head->next->pdata;
}


/*
 *function: insert str to list at the head back 
 *
 *
 */
void inserthead(nodeinfo *head,char*s)
{
   if(NULL==head ||NULL==s)
   {
	return;
   }
   
   int len=strlen(s);
   global_listsize+=(len+1);
   nodeinfo *ptmp;
   ptmp=(nodeinfo*)malloc(sizeof(nodeinfo));
   if(NULL==ptmp)
   {    
        write_log(logwriteinf,LOG_CRIT,"malloc  faill...");    
   }

   ptmp->pdata=(char*)malloc(len+2);
   memset(ptmp->pdata,'\0',len+1);
   if(NULL==ptmp->pdata)
   {

        write_log(logwriteinf,LOG_CRIT,"malloc  faill...");    
   }
   memcpy(ptmp->pdata,s,len);
   ptmp->next=head->next;
   head->next=ptmp;  
}


/*
 *function: free node
 *
 */
void freenode(nodeinfo *p )
{
     if(NULL == p) return;
     free(p);
     p=NULL;
}


/*
 *function: clean list node except head node
 *
 */
void listclean(nodeinfo *head)
{
    if(head==NULL)
    {
        return;
    }
    nodeinfo *p1=head->next;
    nodeinfo *p2=NULL;

    while(p1!=NULL)
    {
         p2=p1;
         p1=p1->next;
         free(p2->pdata);
         p2->pdata=NULL;
         free(p2);
         p2=NULL;
    }   
    head->next=NULL;
    global_listsize=0;
}



/*
 *function: delete all list data when main reboot or down
 *
 */
void listdestroy(nodeinfo*head)
{
    if(head==NULL)
    {
	return ;
    }
    listclean(head);
    free(head);
    head=NULL;
}



/*
 *function: check list exist data ?when main exit
 *if the list not empty,write  it local file 
 *
 */

void writefile(nodeinfo *head)
{
     if(getfilesize(global_dflogpath)>0)
     {
	  unlink(global_dflogpath);
     }

     int fd=open(global_dflogpath,O_WRONLY|O_APPEND|O_CREAT,0666);
     assert(-1 != fd);
     char *buf=NULL;
     nodeinfo *p=head->next; 
     int len=0;
     while(NULL != (buf= getfirstdata(p)))
     {    
          len=strlen(buf);
          buf[len]='\n';
          buf[len+1]='\0';
          write(fd,buf,strlen(buf));
          p=p->next;
          free(buf);
          buf=NULL;
     }
    
    
     close(fd);     
}


/*
 *funcyion: read file t mylist 
 *
 */
void readfile(char*pathname,nodeinfo *head)
{
     int fd=open(pathname,O_RDONLY|O_CREAT,0666);
     assert(-1 != fd);
     char buf[1024]={0};
     while(read(fd,buf,1023) > 0)
     {
        inserthead(head,buf);
        memset(buf,'\0',1024);
     }      
     close(fd);

}


/*
 *function: check kafka queue and write it to  local file
 *          if  not queue empty 
 */

//#define RD_POLL_INFINITE -1

void kafkaqueuetof(rd_kafka_t**rks,int rkcount)
{
   int i=0;
   int fd=open(global_dflogpath,O_WRONLY|O_APPEND|O_CREAT,0666);
   assert(-1 != fd);
   rd_kafka_op_t *rko=NULL;
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
 *function: from kafka queue to list when main exit and queue not null
 *
 */
void qreadtol(nodeinfo *head)
{
   readfile(global_dflogpath,head);
}



/*
 *function: read data in file when main reboot or down, in older to data all
 *
 */
void lreadtol(nodeinfo*head)
{
     readfile(global_dflogpath,head);
}

int main (int argc, char **argv)
{
	rd_kafka_t *rks[1024] = { 0 };
	int  rkcount = 0;
	int  rk = 0;
	char value[1024] = { 0 };
	char brokers[1024] = "localhost:9092";
	char *broker = NULL;
	char *topic = NULL;
	int partitions = 4;
	int partition = 0;
	int i;
	int opt;

        nodeinfo *head;
        listinit(&head);
	topic = "syslog";

	partition = 0;
        
	if (read_config("brokers", value, sizeof(value), "/etc/sendkafka.conf") > 0) {
		strcpy(brokers, value);
                memset(value,'\0',1024);
	}
	if (read_config("topic", value, sizeof(value), "/etc/sendkafka.conf") > 0) {
		topic = strdup(value);
                memset(value,'\0',1024);
	}
	if (read_config("partitions", value, sizeof(value), "/etc/sendkafka.conf") > 0) {
		partitions = atoi(value);
		if (partitions <= 0 || partitions > 256) {
			partitions = 4;
		}
	}
  
        if (read_config("daflogpath", value, sizeof(value), "/etc/sendkafka.conf") > 0) {
		strcpy(global_dflogpath, value);
		memset(value,'\0',1024);
	}
	if (read_config("erflogpath", value, sizeof(value), "/etc/sendkafka.conf") > 0) {
		strcpy(global_eflogpath, value);
		memset(value,'\0',1024);
	}
	if (read_config("erfslogpath", value, sizeof(value), "/etc/sendkafka.conf") > 0) {
		strcpy(global_efslogpath, value);
		 memset(value,'\0',1024);
	}
	if (read_config("logwrite", value, sizeof(value), "/etc/sendkafka.conf") > 0) {
		logwriteinf=atoi(value);
	}
	if (read_config("listmaxsize", value, sizeof(value), "/etc/sendkafka.conf") > 0) {
		listmaxsize=logwriteinf=atoi(value);
	}

	if (read_config("logmaxnum", value, sizeof(value), "/etc/sendkafka.conf") > 0) {
		logmaxnum=logwriteinf=atoi(value);
	}

	while ((opt = getopt(argc, argv, "hb:c:d:")) != -1) {
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
                        
			if (read_config("daflogpath", value, sizeof(value), optarg) > 0) {
				
				strcpy(global_dflogpath, value);
                                memset(value,'\0',1024);
			}
			if (read_config("erflogpath", value, sizeof(value), optarg) > 0) {
				
				strcpy(global_eflogpath, value);
                                memset(value,'\0',1024);
			}
			if (read_config("erfslogpath", value, sizeof(value), optarg) > 0) {
				
				strcpy(global_efslogpath, value);
                                memset(value,'\0',1024);
			}
                        
			if (read_config("logwrite", value, sizeof(value), optarg) > 0) {
				
                                logwriteinf=atoi(value);
			}
			if (read_config("logmaxnum", value, sizeof(value), optarg) > 0) {
				
                                logmaxnum=atoi(value);
			}
			break;

                case 'o':
                        if(NULL!=optarg){
                                logwriteinf=atoi(optarg);
			}
			break;    
		case 't':
			if(NULL!=optarg){
				strcpy(topic, optarg);
			}
			break;
		case 'p':
			if(NULL!=optarg){
				partitions=atoi(optarg);
			}
			break;
		case 'l':
			if(NULL!=optarg){
				strcpy(global_eflogpath, optarg);
			}
			break;

		case 's':
			if(NULL!=optarg){
				strcpy(global_efslogpath, optarg);
			}
			break;
		case 'd':
			if(NULL!=optarg){
				strcpy(global_dflogpath, optarg);
			}
			break;
		case 'm':
			if(NULL!=optarg){
				listmaxsize=atoi(optarg);
			}
			break;
		case 'n':
			if(NULL!=optarg){
				logmaxnum=atoi(optarg);
			}
			break;
		case 'h':
		default:
			usage(argv[0]);
			break;
		}
	}
        
	
        write_log(logwriteinf,0,NULL);

        lreadtol(head);
        qreadtol(head);

	signal(SIGINT, stop);
	signal(SIGTERM, stop);
	// see: https://github.com/edenhill/librdkafka/issues/2
	signal(SIGPIPE, SIG_IGN);

	signal(SIGHUP,stop);//当挂起进程的控制终端的时候,当接收到此类信号的时候正常退出 
	
       /* Producer
	 */
	char buf[4096];
	int sendcnt = 0;
	int ret = 0;

	/* Create Kafka handle */
	for (broker = strtok(brokers, ","), rkcount = 0;
	     broker && rkcount < sizeof(rks);
	     broker = strtok(NULL, ","), rkcount++) 
	{
		rks[rkcount] = rd_kafka_new(RD_KAFKA_PRODUCER, broker, NULL);
		if (!rks[rkcount]) {
			for (i = 0; i < rkcount; i++) {
				rd_kafka_destroy(rks[i]);
				rks[i] = NULL;
			}

                        strcpy(buf,getcurrenttime1());
                        strncat(buf,"kafka_new producer is fail...",29);                   
			perror(buf);
        		write_log(logwriteinf,LOG_CRIT,buf);    
			run=0;

		}

	}
	srand(time(NULL));
	while ( run ) 
	{
		fgets(buf, sizeof(buf), stdin);
		if(EINTR==errno){
			continue;
		}

  		if( global_listsize >=  listmaxsize )
                {
                     sleep(2);
                }

                inserthead(head,buf);
                char *opbuf=NULL;
                int len=0;

                if(NULL != (opbuf=getfirstdata(head)))
                {   
                    len=strlen(opbuf);
                }

		/* Random rk and partition */
		rk = rand() % rkcount;
		partition = rand() % partitions;

		/* Send/Produce message. */
		ret = rd_kafka_produce(rks[rk], topic, partition, RD_KAFKA_OP_F_FREE, opbuf, len);
		if (ret != 0) {
			rk ++;
			rk = rk % rkcount;
			ret = rd_kafka_produce(rks[rk], topic, partition, RD_KAFKA_OP_F_FREE, opbuf, len);
			if (ret != 0) {
				fprintf(stderr, "%s sendkafka[%d]: failed: %s\n",getcurrenttime1(),getpid(), opbuf);
        			char *buf=calloc(1,strlen(opbuf)+128);
				sprintf(buf,"%s sendkafka[%d]: failed: %s\n",getcurrenttime1(),getpid(), opbuf);
			 	write_log(logwriteinf,LOG_INFO,buf);    
				free(buf);
				buf=NULL;
			}
		}

                if(0==ret)
                {
                    delfirstnode(head);
		    sendcnt++;
                }
                
		if (sendcnt % 100000 == 0) {
		     fprintf(stderr, "%s sendkafka[%d]: Sent %i messages to topic %s\n", getcurrenttime1(),getpid(), sendcnt, topic);

		     char *buf=calloc(1,strlen(topic)+128);
		     sprintf(buf,"%s sendkafka[%d]: failed: %s\n",getcurrenttime1(),getpid(), opbuf);                                write_log(logwriteinf,LOG_INFO,buf);    
		     free(buf);
		     buf=NULL;
		}
				
	}

         printf("sendcnt  is :  %d\n",sendcnt);
         printf("rkcount  is :  %d\n",rkcount);
	/* Wait for messaging to finish. */
	/*for (i = 0; i < rkcount; i ++) {
		while (rd_kafka_outq_len(rks[i]) > 0)
			usleep(50000);
	}
        */
	/* Since there is no ack for produce messages in 0.7
	 * we wait some more for any packets to be sent.
	 * This is fixed in protocol version 0.8 */
          
	//if (sendcnt > 0)
//		usleep(500000);

	/* Destroy the handle */
	for (i = 0; i < rkcount; i ++) {
		rd_kafka_destroy(rks[i]);
	}
       
        writefile(head);
        listdestroy(head);
       
        kafkaqueuetof(rks,rkcount);
        
	return 0;

}
