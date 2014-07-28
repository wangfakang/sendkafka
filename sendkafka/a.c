#include <ctype.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static int run = 1;

static void stop (int sig) {
	run = 0;
}

int main (int argc, char **argv) {

time_t t = time(NULL);
printf("%s\n", asctime(localtime(&t)));
//exit(0);

FILE *fp = fopen("/tmp/sendkafka.tmp", "a");
if (fp == NULL) {
	perror("failed to open tmp file");
	exit(1);
}

	signal(SIGINT, stop);
	signal(SIGPIPE, SIG_IGN);

	char buf[2048];
	int sendcnt = 0;

	while (run && (fgets(buf, sizeof(buf), stdin))) {
		int len = strlen(buf);
		char *opbuf = malloc(len + 1);
		strncpy(opbuf, buf, len + 1);

		fprintf(fp, "%s", buf);

		sendcnt++;
	}

fclose(fp);

	return 0;
}
