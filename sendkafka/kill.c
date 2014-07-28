#include<signal.h>
#include<stdio.h>
#include<unistd.h>
#include<assert.h>


int main(int argc,char*argv[])
{
    int id=0;
    int sig=0;
    if(argc>=3)
    {
        sscanf(argv[2],"%d",&sig);
        printf("%d\n",sig);
    }
    
    sscanf(argv[1],"%d",&id);
    printf("%d\n",id);  
    int res= kill(id,SIGINT);
    
    assert(-1!=res);
    return 0;
}


