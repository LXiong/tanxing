/**
 * @file daemonize.c
 * @author zhouyuefeng
 * @date 2014/06/11 16:58:12
 * @brief a framework for daemon process
 *  
 **/

#include <stdio.h>
#include <fcntl.h>
#include <signal.h>
#include <unistd.h>
#include <time.h>

#include "daemonize.h"

char *daemonizeLog = "daemonize.log";
char *daemonizeLock = "daemonize.lock";

void log_message(const char *filename, const char *message)
{
    if(!filename){
        filename = daemonizeLog;
    }
    FILE *logfile;
    logfile=fopen(filename,"a");
    if(!logfile) return;

    time_t rawtime;
    struct tm* timeinfo;
    time(&rawtime);
    timeinfo = localtime(&rawtime);
    char *timeString = strdup(asctime(timeinfo));
    int len;
    if((len = strlen(timeString)) > 0){
        timeString[len - 1] = 0; // cut off the '\n'
    }
    fprintf(logfile,"[%s] %s\n", timeString, message);
    fclose(logfile);
}

void daemonExit(int status){
    int lfp = open(daemonizeLock, O_WRONLY|O_TRUNC);
    if(lfp >= 0){
        close(lfp);
    }
    else{
        log_message(daemonizeLog, "cannot reset lockfile");
    }
    char buf[100];
    sprintf(buf, "daemon %d exit(%d)", getpid(), status);
    log_message(daemonizeLog, buf);
    exit(status);
}

void signal_handler(int sig)
{
    switch(sig) {
    case SIGHUP:
        log_message(daemonizeLog, "hangup signal catched");
        break;
    case SIGTERM:
        log_message(daemonizeLog, "terminate signal catched");
        daemonExit(0);
        break;
    }
}

void daemonize(const char* runningDir, const char* logFile, const char* lockFile)
{
    int i = 0;
    char str[100];

    if(!runningDir){
        runningDir = ".";
    }
    if(logFile){
        daemonizeLog = strdup(logFile);
    }
    if(lockFile){
        daemonizeLock = strdup(lockFile);
    }

    if(getppid() == 1) return; /* already a daemon */

    i = fork();
    if (i < 0) exit(1); /* fork error */
    if (i > 0) exit(0); /* parent exits */

    setsid(); /* obtain a new process group */

    for(i = getdtablesize(); i>=0; --i){
        close(i); /* close all descriptors */
    }
    i = open("/dev/null",O_RDWR); dup(i); dup(i); /* handle standart I/O */

    umask(027); /* set newly created file permissions */

    chdir(runningDir); /* change running directory */

    i = open(daemonizeLock, O_RDWR|O_CREAT, 0640);
    if(i < 0){
        log_message(daemonizeLog,"cannot open lock file, exit soon");
        exit(1); /* can not open */
    }
    if(lockf(i, F_TLOCK, 0) < 0){
        log_message(daemonizeLog,"cannot lock lock file, already running? exit soon");
        exit(0); /* can not lock */
    }

    sprintf(str,"%d\n",getpid());
    write(i, str, strlen(str)); /* record pid to lockfile */

    signal(SIGCHLD,SIG_IGN); /* ignore child */
    signal(SIGTSTP,SIG_IGN); /* ignore tty signals */
    signal(SIGTTOU,SIG_IGN);
    signal(SIGTTIN,SIG_IGN);
    signal(SIGHUP,signal_handler); /* catch hangup signal */
    signal(SIGTERM,signal_handler); /* catch kill signal */

    sprintf(str,"process %d daemonized.", getpid());
    log_message(daemonizeLog, str);
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
