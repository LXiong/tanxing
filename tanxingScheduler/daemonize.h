/**
 * @file daemonize.h
 * @author zhouyuefeng
 * @date 2014/06/11 16:58:12
 * @brief a framework for daemon process
 *  
 **/

#ifndef daemonize__h
#define daemonize__h

void log_message(const char *filename, const char *message);

void daemonize(const char* runningDir, const char* logFile, const char* lockFile);

void daemonExit(int status);

#endif

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
