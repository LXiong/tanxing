/**
 * @file main.c
 * @author zhouyuefeng
 * @date 2014/06/13 15:59:51
 * @brief 
 *  
 **/

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <mysql.h>
#include <string.h>
#include <time.h>
#include "daemonize.h"
#include "cJSON.h"

#define TSTAT_NEW 1
#define TSTAT_WAIT 2
#define TSTAT_RUN 3
#define TSTAT_DONE 4 
#define TSTAT_FAIL 5 
#define TSTAT_TOKILL 6 
#define TSTAT_KILLED 7


const char *workspace;

struct taskList{
    char taskDir[1024];
    struct taskList *next;
};

struct taskInfo{
    int len;
    int *id;
    struct taskList** list;
} runningTask;

void deleteTaskInfo(int i){
struct taskList* node = runningTask.list[i];
    struct taskList* next = node->next;
    while(node){
        next = node->next;
        free(node);
        node = next;
    }
    runningTask.len --;
    runningTask.id[i] = runningTask.id[runningTask.len];
    runningTask.list[i] = runningTask.list[runningTask.len];
    runningTask.id[runningTask.len] = 0;
    runningTask.list[runningTask.len] = NULL;
}

void print_help(){
    puts("  OPTIONS:\n\
    -h : show this help\n\
    -c FILE : specify configuration file\n");
}

int checkStatus(int idx){
    int done = 1;
    char *shortBuffer = (char*)malloc(1024);
    struct taskList *subTask = runningTask.list[idx];
    for(subTask = runningTask.list[idx]; subTask; subTask = subTask->next){
        sprintf(shortBuffer, "./checkDone.sh %s", subTask->taskDir);
        system(shortBuffer);
        
        FILE *f = fopen("checkDone.result", "rb");
        fseek(f, 0, SEEK_END);
        long len = ftell(f);
        fseek(f, 0, SEEK_SET);
        char *data=(char*)malloc(len + 1);
        fread(data, 1, len, f);
        int ret = atoi(data);
        free(data);
        fclose(f);

        sprintf(shortBuffer, "./checkDone.sh %s | state: %d", subTask->taskDir, ret);
        log_message(0, shortBuffer);
        if(ret == TSTAT_FAIL){
            free(shortBuffer);
            return TSTAT_FAIL;
        }
        if(ret == TSTAT_RUN){
            done = 0;
        }
    }
    free(shortBuffer);
    if(done){
        return TSTAT_DONE;
    }
    return TSTAT_RUN;
}

int existSameTask(int id)
{
    int i;
    int flag = 0;
    for(i = 0; i < runningTask.len; i++){
        if( runningTask.id[i] == id ){
           flag = 1; 
        }
    }
    return flag;
}

void killTask(int id, MYSQL *connTaskInfo){
    int i;
    char *shortBuffer = (char*)malloc(1024);
    for(i = 0; i < runningTask.len; i++){
        if(runningTask.id[i] == id){
            log_message(0, "id found in running list");
            struct taskList *subTask = runningTask.list[i];
            for(subTask = runningTask.list[i]; subTask; subTask = subTask->next){
                sprintf(shortBuffer, "./killTask.sh %s", subTask->taskDir);
                log_message(0, shortBuffer);
                system(shortBuffer);
            }
            deleteTaskInfo(i);
            break;
        }
    }
    sprintf(shortBuffer,"update task set status = %d, update_datetime= now() where id = %d", TSTAT_KILLED, id);
    if (mysql_query(connTaskInfo, shortBuffer)) {
        sprintf(shortBuffer, "%s\n", mysql_error(connTaskInfo));
        log_message(0, shortBuffer);
        daemonExit(1);
    }
    free(shortBuffer);
}

cJSON *loadJsonFile(const char* filename){
    FILE *f = fopen(filename, "rb");
    fseek(f, 0, SEEK_END);
    long len = ftell(f);
    fseek(f, 0, SEEK_SET);
    char *data=(char*)malloc(len + 1);
    fread(data, 1, len, f);
    fclose(f);
    cJSON *ret = cJSON_Parse(data);
    free(data);
    return ret;
}

//add Task3 to runningtask while ts restarting
void addTask3ToRunningTask(int id, MYSQL *connTaskInfo)
{
    char *trade = NULL;
    MYSQL_RES *res;
    MYSQL_ROW row;
    int trade_level = 0;
    char *mediumBuffer=(char *)malloc(1024);
    sprintf(mediumBuffer, "select dim_type, dim_value_string from task_criteria where task_id = %d and is_deleted = 0",id);
    if (mysql_query(connTaskInfo, mediumBuffer)) {
        sprintf(mediumBuffer, "%s\n", mysql_error(connTaskInfo));
        log_message(0, mediumBuffer);
        daemonExit(1);
    }
    res = mysql_use_result(connTaskInfo);
    while ((row = mysql_fetch_row(res)) != NULL){
        if(strncmp(row[0],"trade", 5 ) == 0){
            trade = strdup(row[1]);
            trade_level = row[0][5] - '0';
        }
    }
    struct taskList** subtask = &(runningTask.list[runningTask.len]);
    runningTask.id[runningTask.len] = id;
    runningTask.len ++;
 
    if(trade_level < 5){
        char *tokTrade = (char *)strtok(trade, ",");
        while(tokTrade){
            (*subtask) = (struct taskList*) malloc(sizeof(struct taskList));
            (*subtask)->next = NULL;
            sprintf((*subtask)->taskDir, "%s/task_%d_%s", workspace, id, tokTrade);
            log_message(0, (*subtask)->taskDir); 
            subtask = &((*subtask)->next);
            tokTrade = strtok(NULL, ",");
        }
    }        
    else{
        cJSON *tradeJson = cJSON_Parse(trade);
        cJSON *tradeIter = tradeJson->child;

        for(;tradeIter;tradeIter = tradeIter->next){
            char *customerID = tradeIter->string;
            char *accountList = cJSON_PrintUnformatted(tradeIter);
            accountList = strtok(accountList, "[]");
            log_message(0, accountList);
            (*subtask) = (struct taskList*) malloc(sizeof(struct taskList));
            (*subtask)->next = NULL;
            sprintf((*subtask)->taskDir, "%s/task_%d_%s", workspace, id, customerID);
            log_message(0, (*subtask)->taskDir);
            subtask = &((*subtask)->next);
        } 
    }
    mysql_free_result(res);
    if(trade) free(trade);
    if(mediumBuffer) free(mediumBuffer);
}

void doTask3(int id, const char* result_type, MYSQL *connTaskInfo, const char *templateDir){
    char *shortBuffer = (char*)malloc(1024);
    char *longBuffer = (char*)malloc(65536);
    char *dateFormat = (char*)malloc(100);
    MYSQL_RES *res;
    MYSQL_ROW row;
    char *date = NULL;
    char *province = NULL;
    char *city = NULL;
    char *channel = NULL;
    cJSON *tradeJson;
    //char *trade=(char*)malloc(65536);
    char *trade = NULL;
    int trade_level = 0;
    int same_task_flag=0;

    sprintf(shortBuffer, "select dim_type, dim_value_string from task_criteria where task_id = %d and is_deleted = 0", id);
    if(mysql_query(connTaskInfo, shortBuffer)){
        sprintf(shortBuffer, "%s\n", mysql_error(connTaskInfo));
        log_message(0, shortBuffer);
        daemonExit(1);
    }
    res = mysql_use_result(connTaskInfo);
    
    while ((row = mysql_fetch_row(res)) != NULL){
        sprintf(shortBuffer,"task_criteria: %s\t%s", row[0], row[1]);
        //log_message(0, shortBuffer);
        if(strcmp(row[0],"date_range") == 0){
            date = strdup(row[1]);
            int i=0;
            char *p=date;
            while( *p != '\0' ){
                if( *p != '-' ){
                    dateFormat[i]=*p;
                    i++;
                }
                p++;
            }            
            dateFormat[i]='\0';
        }
        else if(strcmp(row[0],"province") == 0){
            province = strdup(row[1]);
        }
        else if(strcmp(row[0],"city") == 0){
            city = strdup(row[1]);
        }
        else if(strcmp(row[0],"channel") == 0){
            channel = strdup(row[1]);
        }
        else if(strncmp(row[0],"trade", 5 ) == 0){
//trade = strdup(row[1]);
            trade_level = row[0][5] - '0';
            if(trade_level<5)
                trade = strdup(row[1]);
            else
                tradeJson = cJSON_Parse(row[1]);

        }
    }
    if(!province) province=strdup("0");
    if(!city) city=strdup("0");
    if(!channel) channel=strdup("0");
    same_task_flag = existSameTask(id);
    struct taskList** subtask = &(runningTask.list[runningTask.len]);
    if(!same_task_flag){
        runningTask.id[runningTask.len] = id;
        runningTask.len ++;
    }

    if(trade_level < 5){
        char *tokTrade = (char *)strtok(trade, ",");
        while(tokTrade){
            sprintf(longBuffer, "./extractTemplate.py -c %s/conf/base.conf -c %s/conf/jobx.conf -w %s/task_%d_%s -v subtask_id=task_%d_%s -v date=%s -v province=%s -v city=%s -v channel=%s -v trade=%s -v trade_level=%d -s goalset=%s", templateDir, templateDir, workspace, id, tokTrade, id, tokTrade, dateFormat, province, city, channel, tokTrade, trade_level, result_type);
            log_message(0, longBuffer);
            system(longBuffer);
            
            if(!same_task_flag){
               (*subtask) = (struct taskList*) malloc(sizeof(struct taskList));
               (*subtask)->next = NULL;
               sprintf((*subtask)->taskDir, "%s/task_%d_%s", workspace, id, tokTrade);
               subtask = &((*subtask)->next);
            }
            tokTrade = strtok(NULL, ",");
        }
    }
    else{
        log_message(0, "We got a account level job.");
        //cJSON *tradeJson = cJSON_Parse(trade);
        cJSON *tradeIter = tradeJson->child;

        for(;tradeIter;tradeIter = tradeIter->next){
            char *customerID = tradeIter->string;
            log_message(0, customerID);
            char *accountList = cJSON_PrintUnformatted(tradeIter);
            accountList = strtok(accountList, "[]");
            log_message(0, accountList);
            char *cmd = "./extractTemplate.py -c template/conf/base.conf -c template/conf/jobx.conf -w ";
            sprintf(longBuffer, "%s%s/task_%d_%s -v subtask_id=task_%d_%s -v date=%s -v province=%s -v city=%s -v channel=%s -v trade=%s -v trade_level=%d -s goalset=%s", cmd, workspace, id, customerID, id, customerID, date, province, city, channel, accountList, trade_level, result_type);
            log_message(0, longBuffer);

            (*subtask) = (struct taskList*) malloc(sizeof(struct taskList));
            (*subtask)->next = NULL;
            sprintf((*subtask)->taskDir, "%s/task_%d_%s", workspace, id, customerID);
            subtask = &((*subtask)->next);

            system(longBuffer);
        }
        cJSON_Delete(tradeJson);
        log_message(0, "Account relation analysis done.");
    }

    sprintf(shortBuffer,"update task set status = %d, update_datetime = now() where id = %d", TSTAT_RUN, id);
    
    if (mysql_query(connTaskInfo, shortBuffer)) {
        sprintf(shortBuffer, "%s\n", mysql_error(connTaskInfo));
        log_message(0, shortBuffer);
        daemonExit(1);
    }

    if(date) free(date);
    if(province) free(province);
    if(city) free(city);
    if(channel) free(channel);
    if(trade) free(trade);
    mysql_free_result(res);

    struct taskList *x;
    if(!same_task_flag){
        for(x = runningTask.list[runningTask.len - 1]; x; x = x->next){
            log_message(0, x->taskDir);
        }
    }

    //free(shortBuffer);
    free(longBuffer);
    free(dateFormat);
}

void doTask4(int id, const char* result_type, MYSQL *connTaskInfo, const char *templateDir){
    log_message(0, result_type);
    char *shortBuffer = (char*)malloc(1024);
    char *longBuffer = (char*)malloc(65536);
    char *dateFormat = (char*)malloc(100);
    MYSQL_RES *res;
    MYSQL_ROW row;
    char *date = NULL;
    char *province = NULL;
    char *city = NULL;
    char *channel = NULL;
    char *brand = NULL;
    char *file = NULL;
    sprintf(shortBuffer, "select dim_type, dim_value_string from task_criteria where task_id = %d and is_deleted = 0", id);
    if(mysql_query(connTaskInfo, shortBuffer)){
        sprintf(shortBuffer, "%s\n", mysql_error(connTaskInfo));
        log_message(0, shortBuffer);
        daemonExit(1);
    }
    res = mysql_use_result(connTaskInfo);
    
    while ((row = mysql_fetch_row(res)) != NULL){
        sprintf(shortBuffer,"task_criteria: %s\t%s", row[0], row[1]);
        log_message(0, shortBuffer);
        if(strcmp(row[0],"date_range") == 0){
            date = strdup(row[1]);
            int i=0;
            char *p=date;
            while( *p != '\0' ){
                if( *p != '-' ){
                    dateFormat[i]=*p;
                    i++;
                }
                p++;
            }            
            dateFormat[i]='\0';
        }
        else if(strcmp(row[0],"province") == 0){
            province = strdup(row[1]);
        }
        else if(strcmp(row[0],"city") == 0){
            city = strdup(row[1]);
        }
        else if(strcmp(row[0],"channel") == 0){
            channel = strdup(row[1]);
        }
        else if(strncmp(row[0],"brand", 5 ) == 0){
            brand = strdup(row[1]);
        }
        else if(strcmp(row[0],"file_url_1") == 0){
            file=strdup(row[1]);
        }
    }
    if(!province) province=strdup("0");
    if(!city) city=strdup("0");
    if(!channel) channel=strdup("0");
    if(!existSameTask(id)){
         struct taskList** subtask = &(runningTask.list[runningTask.len]);
        runningTask.id[runningTask.len] = id;
        runningTask.len ++;
        (*subtask) = (struct taskList*) malloc(sizeof(struct taskList));
        (*subtask)->next = NULL;
        sprintf((*subtask)->taskDir, "%s/task_%d", workspace, id);
        subtask = &((*subtask)->next);
    }

    char *cmd = "./extractTemplate.py -c template/conf/base.conf -c template/conf/jobx.conf -w ";
    sprintf(longBuffer, "%s %s/task_%d -v subtask_id=task_%d -v brand=%s -v date=%s -v province=%s -v city=%s -v channel=%s -v file=%s -s goalset=%s", cmd, workspace, id, id, brand, date, province, city, channel, file, result_type);
    log_message(0, longBuffer);
    system(longBuffer);


    sprintf(shortBuffer,"update task set status = %d, update_datetime = now() where id = %d", TSTAT_RUN, id);
    
    if (mysql_query(connTaskInfo, shortBuffer)) {
        sprintf(shortBuffer, "%s\n", mysql_error(connTaskInfo));
        log_message(0, shortBuffer);
        daemonExit(1);
    }

    if(date) free(date);
    if(province) free(province);
    if(city) free(city);
    if(channel) free(channel);
    if(brand) free(brand);
    mysql_free_result(res);
    if(file) free(file);
    if(!existSameTask(id)){    
        struct taskList *x;
        for(x = runningTask.list[runningTask.len - 1]; x; x = x->next){
            log_message(0, x->taskDir);
        }
    }

    free(shortBuffer);
    free(longBuffer);
    free(dateFormat);
}

void doTask5(int id, const char* result_type, MYSQL *connTaskInfo, MYSQL *connDict, const char *templateDir){
    log_message(0, result_type);
    char *shortBuffer = (char*)malloc(1024);
    char *longBuffer = (char*)malloc(65536);
    char *dateFormat = (char*)malloc(100);
    MYSQL_RES *res, *resDict;
    MYSQL_ROW row, rowDict;
    char *date = NULL;
    char *account_name_file_http_loc = NULL;
    char *keyword_tags_file_http_loc = NULL;
    
    sprintf(shortBuffer, "select dim_type, dim_value_string from task_criteria where task_id = %d and is_deleted = 0", id);
    if(mysql_query(connTaskInfo, shortBuffer)){
        sprintf(shortBuffer, "%s\n", mysql_error(connTaskInfo));
        log_message(0, shortBuffer);
        daemonExit(1);
    }
    res = mysql_use_result(connTaskInfo);
    
    while ((row = mysql_fetch_row(res)) != NULL){
        sprintf(shortBuffer,"task_criteria: %s\t%s", row[0], row[1]);
        log_message(0, shortBuffer);
        if(strcmp(row[0],"date_range") == 0){
            date = strdup(row[1]);
            int i=0;
            char *p=date;
            while( *p != '\0' ){
                if( *p != '-' ){
                    dateFormat[i]=*p;
                    i++;
                }
                p++;
            }            
            dateFormat[i]='\0';
        }
        else if(strcmp(row[0],"account_list_url") == 0 || strcmp(row[0],"account_soc_url") == 0){
            account_name_file_http_loc = strdup(row[1]);
        }
        else if(strcmp(row[0],"dict_resource_id") == 0){
            sprintf(shortBuffer, "select url from dictionary_upload where id = %d and is_deleted = 0", atoi(row[1]));
            if(mysql_query(connDict, shortBuffer)){
                sprintf(shortBuffer, "%s\n", mysql_error(connDict));
                log_message(0, shortBuffer);
                daemonExit(1);
            }
            resDict = mysql_use_result(connDict);
            if((rowDict = mysql_fetch_row(resDict)) == NULL){
                log_message(0, "rowDict is empty");
            }
            else{
                keyword_tags_file_http_loc = strdup(rowDict[0]);
            }
            mysql_free_result(resDict);
        }
    }
    
    if(!existSameTask(id)){
        struct taskList** subtask = &(runningTask.list[runningTask.len]);
        runningTask.id[runningTask.len] = id;
        runningTask.len ++;

        (*subtask) = (struct taskList*) malloc(sizeof(struct taskList));
        (*subtask)->next = NULL;
        sprintf((*subtask)->taskDir, "%s/task_%d", workspace, id);
        subtask = &((*subtask)->next);
    }

    char *cmd = "./extractTemplate.py -c template/conf/base.conf -c template/conf/jobx.conf -w ";
    sprintf(longBuffer, "%s %s/task_%d -v subtask_id=task_%d -v date=%s -v account_name_file_http_loc=%s -v keyword_accname_tags_file_http_loc=%s -s goalset=%s", cmd, workspace, id, id, date, account_name_file_http_loc, keyword_tags_file_http_loc, result_type);
    log_message(0, longBuffer);
    system(longBuffer);


    sprintf(shortBuffer,"update task set status = %d, update_datetime = now() where id = %d", TSTAT_RUN, id);
    
    if (mysql_query(connTaskInfo, shortBuffer)) {
        sprintf(shortBuffer, "%s\n", mysql_error(connTaskInfo));
        log_message(0, shortBuffer);
        daemonExit(1);
    }

    if(date) free(date);
    if(account_name_file_http_loc) free(account_name_file_http_loc);
    if(keyword_tags_file_http_loc) free(keyword_tags_file_http_loc);

    mysql_free_result(res);
    
    if(!existSameTask(id)){
        struct taskList *x;
        for(x = runningTask.list[runningTask.len - 1]; x; x = x->next){
            log_message(0, x->taskDir);
        }
    }

    free(shortBuffer);
    free(longBuffer);
    free(dateFormat);
}

void doTask7(int id, const char* result_type, MYSQL *connTaskInfo, MYSQL *connDict, const char *templateDir){
    log_message(0, result_type);
    char *shortBuffer = (char*)malloc(1024);
    char *longBuffer = (char*)malloc(65536);
    MYSQL_RES *res, *resDict;
    MYSQL_ROW row, rowDict;

    char *original_query_file_url = NULL;
    char *rule_resource_url = NULL;
    char *dict_resource_url = NULL;
    
    sprintf(shortBuffer, "select dim_type, dim_value_string from task_criteria where task_id = %d and is_deleted = 0", id);
    if(mysql_query(connTaskInfo, shortBuffer)){
        sprintf(shortBuffer, "%s\n", mysql_error(connTaskInfo));
        log_message(0, shortBuffer);
        daemonExit(1);
    }
    res = mysql_use_result(connTaskInfo);
    
    while ((row = mysql_fetch_row(res)) != NULL){
        sprintf(shortBuffer,"task_criteria: %s\t%s", row[0], row[1]);
        log_message(0, shortBuffer);
        if(strcmp(row[0],"original_query_file_url") == 0){
            original_query_file_url = strdup(row[1]);
        }
        else if(strcmp(row[0],"rule_resource_id") == 0 || strcmp(row[0],"account_soc_url") == 0){
            sprintf(shortBuffer, "select url from rule_upload where id = %d and is_deleted = 0", atoi(row[1]));
            if(mysql_query(connDict, shortBuffer)){
                sprintf(shortBuffer, "%s\n", mysql_error(connDict));
                log_message(0, shortBuffer);
                daemonExit(1);
            }
            resDict = mysql_use_result(connDict);
            if((rowDict = mysql_fetch_row(resDict)) == NULL){
                log_message(0, "rowRule is empty");
            }
            else{
                rule_resource_url = strdup(rowDict[0]);
            }
            mysql_free_result(resDict);
        }
        else if(strcmp(row[0],"dict_resource_id") == 0){
            sprintf(shortBuffer, "select url from dictionary_upload where id = %d and is_deleted = 0", atoi(row[1]));
            if(mysql_query(connDict, shortBuffer)){
                sprintf(shortBuffer, "%s\n", mysql_error(connDict));
                log_message(0, shortBuffer);
                daemonExit(1);
            }
            resDict = mysql_use_result(connDict);
            if((rowDict = mysql_fetch_row(resDict)) == NULL){
                log_message(0, "rowDict is empty");
            }
            else{
                dict_resource_url = strdup(rowDict[0]);
            }
            mysql_free_result(resDict);
        }
    }

    if(!existSameTask(id)){
        struct taskList** subtask = &(runningTask.list[runningTask.len]);
        runningTask.id[runningTask.len] = id;
        runningTask.len ++;

        (*subtask) = (struct taskList*) malloc(sizeof(struct taskList));
        (*subtask)->next = NULL;
        sprintf((*subtask)->taskDir, "%s/task_%d", workspace, id);
        subtask = &((*subtask)->next);
    }

    char *cmd = "./extractTemplate.py -c template/conf/base.conf -c template/conf/jobx.conf -w ";
    sprintf(longBuffer, "%s %s/task_%d -v subtask_id=task_%d -v original_query_file_url=%s -v rule_resource_url=%s -v dict_resource_url=%s -s goalset=%s", cmd, workspace, id, id, original_query_file_url, rule_resource_url, dict_resource_url, result_type);
    log_message(0, longBuffer);
    system(longBuffer);

    sprintf(shortBuffer,"update task set status = %d, update_datetime = now() where id = %d", TSTAT_RUN, id);
    
    if (mysql_query(connTaskInfo, shortBuffer)) {
        sprintf(shortBuffer, "%s\n", mysql_error(connTaskInfo));
        log_message(0, shortBuffer);
        daemonExit(1);
    }

    if(original_query_file_url) free(original_query_file_url);
    if(rule_resource_url) free(rule_resource_url);
    if(dict_resource_url) free(dict_resource_url);

    mysql_free_result(res);

    if(!existSameTask(id)){
        struct taskList *x;
        for(x = runningTask.list[runningTask.len - 1]; x; x = x->next){
            log_message(0, x->taskDir);
        }
    }

    free(shortBuffer);
    free(longBuffer);
}

void doTask9(int id, const char* result_type, MYSQL *connTaskInfo, const char *templateDir){
    log_message(0, result_type);
    char *shortBuffer = (char*)malloc(1024);
    char *longBuffer = (char*)malloc(65536);
    char *dateFormat = (char*)malloc(100);
    MYSQL_RES *res, *resDict;
    MYSQL_ROW row, rowDict;
    char *date = NULL;
    char *keyword_list_url = NULL;
    char *channel = NULL;

    sprintf(shortBuffer, "select dim_type, dim_value_string from task_criteria where task_id = %d and is_deleted = 0", id);
    if(mysql_query(connTaskInfo, shortBuffer)){
        sprintf(shortBuffer, "%s\n", mysql_error(connTaskInfo));
        log_message(0, shortBuffer);
        daemonExit(1);
    }
    res = mysql_use_result(connTaskInfo);
    
    while ((row = mysql_fetch_row(res)) != NULL){
        sprintf(shortBuffer,"task_criteria: %s\t%s", row[0], row[1]);
        log_message(0, shortBuffer);
        if(strcmp(row[0],"date_range") == 0){
            date = strdup(row[1]);
            int i=0;
            char *p=date;
            while( *p != '\0' ){
                if( *p != '-' ){
                    dateFormat[i]=*p;
                    i++;
                }
                p++;
            }            
            dateFormat[i]='\0';
        }
        else if(strcmp(row[0],"keyword_list_url") == 0){
            keyword_list_url = strdup(row[1]);
        }
        else if(strcmp(row[0],"channel") == 0){
            channel = strdup(row[1]);
        }
    }

    if(!channel) 
        channel = strdup("0");
    if(!existSameTask(id)){
        struct taskList** subtask = &(runningTask.list[runningTask.len]);
        runningTask.id[runningTask.len] = id;
        runningTask.len ++;

    (*subtask) = (struct taskList*) malloc(sizeof(struct taskList));
    (*subtask)->next = NULL;
    sprintf((*subtask)->taskDir, "%s/task_%d", workspace, id);
    subtask = &((*subtask)->next);
    }

    char *cmd = "./extractTemplate.py -c template/conf/base.conf -c template/conf/jobx.conf -w ";
    sprintf(longBuffer, "%s %s/task_%d -v subtask_id=task_%d -v date=%s -v keyword_list_url=%s -s goalset=%s", cmd, workspace, id, id, date, keyword_list_url, result_type);
    log_message(0, longBuffer);
    system(longBuffer);


    sprintf(shortBuffer,"update task set status = %d,update_datetime = now() where id = %d", TSTAT_RUN, id);
    
    if (mysql_query(connTaskInfo, shortBuffer)) {
        sprintf(shortBuffer, "%s\n", mysql_error(connTaskInfo));
        log_message(0, shortBuffer);
        daemonExit(1);
    }

    if(channel) free(channel);
    if(date) free(date);
    if(keyword_list_url) free(keyword_list_url);

    mysql_free_result(res);

    if(!existSameTask(id)){
        struct taskList *x;
        for(x = runningTask.list[runningTask.len - 1]; x; x = x->next){
            log_message(0, x->taskDir);
        }
    }

    free(shortBuffer);
    free(longBuffer);
    free(dateFormat);
}

void doTask11(int id, const char* result_type, MYSQL *connTaskInfo, const char *templateDir){
    log_message(0, result_type);
    char *shortBuffer = (char*)malloc(1024);
    char *longBuffer = (char*)malloc(65536);
    char *dateFormat = (char*)malloc(100);
    MYSQL_RES *res;
    MYSQL_ROW row;
    char *date = NULL;
    char *channel = (char*)malloc(8);
    char *word_file_url = NULL;
    char *summary_dimension_field = NULL;
    
    sprintf(shortBuffer, "select dim_type, dim_value_string from task_criteria where task_id = %d and is_deleted = 0", id);
    if(mysql_query(connTaskInfo, shortBuffer)){
        sprintf(shortBuffer, "%s\n", mysql_error(connTaskInfo));
        log_message(0, shortBuffer);
        daemonExit(1);
    }
    res = mysql_use_result(connTaskInfo);
    
    while((row = mysql_fetch_row(res)) != NULL){
        sprintf(shortBuffer, "task_criteria: %s\t%s", row[0], row[1]);
        log_message(0, shortBuffer);
        if(strcmp(row[0], "date_range") == 0){
            date = strdup(row[1]);
            int i = 0;
            char *p = date;
            while( *p != '\0' ){
                if( *p != '-' ){
                    dateFormat[i] = *p;
                    i++;
                }
                p++;
            }            
            dateFormat[i] = '\0';
        }
        else if(strcmp(row[0], "word_file_url") == 0){
            word_file_url = strdup(row[1]);
        }        
        else if(strcmp(row[0], "channel") == 0){
            char *temp = strdup(row[1]);
            char *delim = ",";
            char *p = strtok(temp, delim);
            if(strcmp(p,"10011") == 0)
            {
                strcpy(channel, "pc");
            }
            else
            {
                strcpy(channel, "wise");
            }
            while((p = strtok(NULL, delim)))
            {
                strcpy(channel,"pc,wise");
            }
        }
        else if(strcmp(row[0],"summary_dimension_field") == 0){
            summary_dimension_field = strdup(row[1]);
        }
    }
    if(!existSameTask(id)){
        struct taskList** subtask = &(runningTask.list[runningTask.len]);
        runningTask.id[runningTask.len] = id;
        runningTask.len ++;

        (*subtask) = (struct taskList*) malloc(sizeof(struct taskList));
        (*subtask)->next = NULL;
        sprintf((*subtask)->taskDir, "%s/task_%d", workspace, id);
        subtask = &((*subtask)->next);
    }

    char *cmd = "./extractTemplate.py -c template/conf/base.conf -c template/conf/jobx.conf -w ";
    sprintf(longBuffer, "%s %s/task_%d -v subtask_id=task_%d -v channel=%s -v date=%s -v word_file_url=%s -v summary_dimension_field=%s -s goalset=%s", cmd, workspace, id, id, channel, dateFormat, word_file_url, summary_dimension_field, result_type);
    log_message(0, longBuffer);
    system(longBuffer);


    sprintf(shortBuffer,"update task set status = %d, update_datetime= now() where id = %d", TSTAT_RUN, id);
    
    if (mysql_query(connTaskInfo, shortBuffer)) {
        sprintf(shortBuffer, "%s\n", mysql_error(connTaskInfo));
        log_message(0, shortBuffer);
        daemonExit(1);
    }
    if(channel) free(channel);
    if(date) free(date);
    if(word_file_url) free(word_file_url);
    if(summary_dimension_field) free(summary_dimension_field);
    mysql_free_result(res);

    if(!existSameTask(id)){
        struct taskList *x;
        for(x = runningTask.list[runningTask.len - 1]; x; x = x->next){
            log_message(0, x->taskDir);
        }
    }

    free(shortBuffer);
    free(longBuffer);
    free(dateFormat);
}

void doTask10(int id, const char* result_type, MYSQL *connTaskInfo, const char *templateDir){
    log_message(0, result_type);
    char *shortBuffer = (char*)malloc(1024);
    char *longBuffer = (char*)malloc(65536);
    char *cookie_date_range = (char*)malloc(100);
    char *log_date_range = (char*)malloc(100);
    MYSQL_RES *res;
    MYSQL_ROW row;
    int cookie_active_length=0;
    int flag_wordbag_or_rule=0;
    char *channel = NULL;
    char *wordbag_or_rule = NULL;
    int rand_cookie_num=0;
    int cookie_num=0;
    int minimum_pv=0; 
    
    sprintf(shortBuffer, "select dim_type, dim_value_string from task_criteria where task_id = %d and is_deleted = 0", id);
    if(mysql_query(connTaskInfo, shortBuffer)){
        sprintf(shortBuffer, "%s\n", mysql_error(connTaskInfo));
        log_message(0, shortBuffer);
        daemonExit(1);
    }
    res = mysql_use_result(connTaskInfo);
    
    while((row = mysql_fetch_row(res)) != NULL){
        sprintf(shortBuffer, "task_criteria: %s\t%s", row[0], row[1]);
        log_message(0, shortBuffer);
        if(strcmp(row[0], "cookie_date_range") == 0){
            char *temp = strdup(row[1]);
            int i = 0;
            char *p = temp;
            while( *p != '\0' ){
                if( *p != '-' ){
                    cookie_date_range[i] = *p;
                    i++;
                }
                p++;
            }            
            cookie_date_range[i] = '\0';
        }
        else if(strcmp(row[0], "log_date_range") == 0){
            char *temp = strdup(row[1]);
            int i = 0;
            char *p = temp;
            while( *p != '\0' ){
                if( *p != '-' ){
                    log_date_range[i] = *p;
                    i++;
                }
                p++;
            }
            log_date_range[i] = '\0';
        }
        else if(strcmp(row[0], "cookie_time") == 0){
            cookie_active_length = atoi(row[1]);
        }
        else if(strcmp(row[0], "include_word_file_url") == 0){
            flag_wordbag_or_rule = 0;
            wordbag_or_rule = strdup(row[1]);
        }    
        else if(strcmp(row[0], "query_sequence_rule") == 0){
            flag_wordbag_or_rule = 1;
            wordbag_or_rule = strdup(row[1]);
        }
        else if(strcmp(row[0], "channel") == 0){
            channel = strdup(row[1]);
        }
        else if(strcmp(row[0],"minimum_pv") == 0){
            minimum_pv = atoi(row[1]);
        }
        else if(strcmp(row[0],"pv_sample_number") == 0){
            cookie_num = atoi(row[1]);
        }
        else if(strcmp(row[0],"random_sample_number") == 0){
            rand_cookie_num = atoi(row[1]);
        }
    }

    if(!existSameTask(id)){
        struct taskList** subtask = &(runningTask.list[runningTask.len]);
        runningTask.id[runningTask.len] = id;
        runningTask.len ++;
    
        (*subtask) = (struct taskList*) malloc(sizeof(struct taskList));
        (*subtask)->next = NULL;
        sprintf((*subtask)->taskDir, "%s/task_%d", workspace, id);
        subtask = &((*subtask)->next);
    }

    char *cmd = "./extractTemplate.py -c template/conf/base.conf -c template/conf/jobx.conf -w ";
    sprintf(longBuffer, "%s %s/task_%d -v subtask_id=task_%d -v channel=%s -v cookie_date_range=%s -v  cookie_active_length=%d -v flag_wordbag_or_rule=%d -v wordbag_or_rule=%s -v minimum_pv=%d -v cookie_num=%d -v rand_cookie_num=%d -v log_date_range=%s -s goalset=%s", cmd, workspace, id, id, channel, cookie_date_range, cookie_active_length, flag_wordbag_or_rule, wordbag_or_rule, minimum_pv, cookie_num, rand_cookie_num, log_date_range, result_type);
    log_message(0, longBuffer);
    system(longBuffer);


    sprintf(shortBuffer,"update task set status = %d, update_datetime= now() where id = %d", TSTAT_RUN, id);
    
    if (mysql_query(connTaskInfo, shortBuffer)) {
        sprintf(shortBuffer, "%s\n", mysql_error(connTaskInfo));
        log_message(0, shortBuffer);
        daemonExit(1);
    }
    if(channel) free(channel);
    if(cookie_date_range) free(cookie_date_range);
    if(wordbag_or_rule) free(wordbag_or_rule);
    if(log_date_range) free(log_date_range);
    mysql_free_result(res);

    if(!existSameTask(id)){
        struct taskList *x;
        for(x = runningTask.list[runningTask.len - 1]; x; x = x->next){
            log_message(0, x->taskDir);
        }
    }

    free(shortBuffer);
    free(longBuffer);
}

void doTask12(int id, const char* result_type, MYSQL *connTaskInfo, const char *templateDir){
    log_message(0, result_type);
    char *shortBuffer = (char*)malloc(1024);
    char *longBuffer = (char*)malloc(65536);
    char *dateFormat = (char*)malloc(100);
    MYSQL_RES *res;
    MYSQL_ROW row;
    char *date = NULL;
    char *area = NULL;
    char *channel = NULL;
    char *word_file_url = NULL;
    char *summary_dimension_field = NULL;
    
    sprintf(shortBuffer, "select dim_type, dim_value_string from task_criteria where task_id = %d and is_deleted = 0", id);
    if(mysql_query(connTaskInfo, shortBuffer)){
        sprintf(shortBuffer, "%s\n", mysql_error(connTaskInfo));
        log_message(0, shortBuffer);
        daemonExit(1);
    }
    res = mysql_use_result(connTaskInfo);
    
    while((row = mysql_fetch_row(res)) != NULL){
        sprintf(shortBuffer, "task_criteria: %s\t%s", row[0], row[1]);
        log_message(0, shortBuffer);
        if(strcmp(row[0], "date_range") == 0){
            date = strdup(row[1]);
            int i = 0;
            char *p = date;
            while( *p != '\0' ){
                if( *p != '-' ){
                    dateFormat[i] = *p;
                    i++;
                }
                p++;
            }            
            dateFormat[i] = '\0';
        }
        else if(strcmp(row[0], "word_file_url") == 0){
            word_file_url = strdup(row[1]);
        }        
        else if(strcmp(row[0], "channel") == 0){
            channel = strdup(row[1]);
        }
        else if(strcmp(row[0], "province") == 0){
            area = strdup(row[1]);
        }
        else if(strcmp(row[0], "city") == 0){
            area = strdup(row[1]);
        }
        else if(strcmp(row[0],"dimension") == 0){
            summary_dimension_field = strdup(row[1]);
        }
    }
    if(!existSameTask(id)){
        struct taskList** subtask = &(runningTask.list[runningTask.len]);
        runningTask.id[runningTask.len] = id;
        runningTask.len ++;

        (*subtask) = (struct taskList*) malloc(sizeof(struct taskList));
        (*subtask)->next = NULL;
        sprintf((*subtask)->taskDir, "%s/task_%d", workspace, id);
        subtask = &((*subtask)->next);
    }

    char *cmd = "./extractTemplate.py -c template/conf/base.conf -c template/conf/jobx.conf -w ";
    sprintf(longBuffer, "%s %s/task_%d -v subtask_id=task_%d -v channel=%s -v date=%s -v word_file_url=%s -v dimension=%s -v area=%s -s goalset=%s", cmd, workspace, id, id, channel, dateFormat, word_file_url, summary_dimension_field, area, result_type);
    log_message(0, longBuffer);
    system(longBuffer);
     
    sprintf(shortBuffer,"update task set status = %d , update_datetime = now() where id = %d", TSTAT_RUN, id);
    
    if (mysql_query(connTaskInfo, shortBuffer)) {
        sprintf(shortBuffer, "%s\n", mysql_error(connTaskInfo));
        log_message(0, shortBuffer);
        daemonExit(1);
    }
    if(channel) free(channel);
    if(date) free(date);
    if(area) free(area);
    if(word_file_url) free(word_file_url);
    if(summary_dimension_field) free(summary_dimension_field);
    mysql_free_result(res);

    if(!existSameTask(id)){
        struct taskList *x;
        for(x = runningTask.list[runningTask.len - 1]; x; x = x->next){
            log_message(0, x->taskDir);
        }
    }

    free(shortBuffer);
    free(longBuffer);
    free(dateFormat);
}

int main(int argc, char** argv){
    int i;
    char *shortBuffer = (char*) malloc(1024); // free on exit
    char *mediumBuffer = (char*) malloc(1024);
    int opt = 0;
    char *optString = "hc:";
    char *configurationFile = "default.conf";
    int concurrencyLimit = 0;
    MYSQL *conn1, *conn2, *conn_dict;
    MYSQL_RES *res;
    MYSQL_ROW row;

    // daemonize process.
    daemonize(0,0,0);

    // parse options
    while((opt = getopt(argc, argv, optString)) != -1){
        switch(opt){
            case 'h':
                print_help();
                exit(0);
                break;
            case 'c':
                configurationFile = strdup(optarg);
                break;
            default:
                break;
        }
    }

    // parse configuration file
    cJSON* conf = loadJsonFile(configurationFile);
    if(!conf){
        printf("Error parsing json file <%s> before: [%s]\n", configurationFile, cJSON_GetErrorPtr());
        exit(1);
    }
    cJSON* dbTaskInfo = cJSON_GetObjectItem(cJSON_GetObjectItem(conf, "database"), "dbTaskInfo");
    char *dbTaskInfo_host = strdup(cJSON_GetObjectItem(dbTaskInfo, "host")->valuestring); // free on exit
    unsigned dbTaskInfo_port = (unsigned)cJSON_GetObjectItem(dbTaskInfo, "port")->valueint;
    char *dbTaskInfo_database = strdup(cJSON_GetObjectItem(dbTaskInfo, "database")->valuestring); // free on exit
    char *dbTaskInfo_user = strdup(cJSON_GetObjectItem(dbTaskInfo, "user")->valuestring); // free on exit
    char *dbTaskInfo_password = strdup(cJSON_GetObjectItem(dbTaskInfo, "password")->valuestring); // free on exit

    cJSON* db_bim_rap_result_db = cJSON_GetObjectItem(cJSON_GetObjectItem(conf, "database"), "db_bim_rap_result_db");
    char *db_bim_rap_result_db_host = strdup(cJSON_GetObjectItem(db_bim_rap_result_db, "host")->valuestring); // free on exit
    unsigned db_bim_rap_result_db_port = (unsigned)cJSON_GetObjectItem(db_bim_rap_result_db, "port")->valueint;
    char *db_bim_rap_result_db_database = strdup(cJSON_GetObjectItem(db_bim_rap_result_db, "database")->valuestring); // free on exit
    char *db_bim_rap_result_db_user = strdup(cJSON_GetObjectItem(db_bim_rap_result_db, "user")->valuestring); // free on exit
    char *db_bim_rap_result_db_password = strdup(cJSON_GetObjectItem(db_bim_rap_result_db, "password")->valuestring); // free on exit

    char *templateDir = strdup(cJSON_GetObjectItem(conf, "templateDir")->valuestring); // free on exit
    workspace = strdup(cJSON_GetObjectItem(conf, "workspace")->valuestring); // free on exit
    concurrencyLimit = cJSON_GetObjectItem(conf, "concurrencyLimit")->valueint;

    cJSON_Delete(conf);
    log_message(0, "configuration file loaded.");

    // initialize states
    runningTask.len = 0;
    runningTask.id = (int*)malloc(sizeof(int) * concurrencyLimit);
    runningTask.list = (struct taskList**)malloc(sizeof(struct taskList*) * concurrencyLimit);
    memset(runningTask.id, 0, sizeof(int) * concurrencyLimit);
    memset(runningTask.list, 0, sizeof(struct taskList*) * concurrencyLimit);

    // make workspace directory
    struct stat st = {0};
    if (stat(workspace, &st) == -1) {
        mkdir(workspace, 0700);
    } 

    // establish database connections
    conn1 = mysql_init(NULL);
    conn2 = mysql_init(NULL);
    conn_dict = mysql_init(NULL);
    
    my_bool reconnect = 1;
    mysql_options(conn1, MYSQL_OPT_RECONNECT, &reconnect);
    reconnect = 1;
    mysql_options(conn2, MYSQL_OPT_RECONNECT, &reconnect);
    reconnect = 1;
    mysql_options(conn_dict, MYSQL_OPT_RECONNECT, &reconnect);
    if (!mysql_real_connect(conn1, dbTaskInfo_host, dbTaskInfo_user, dbTaskInfo_password, dbTaskInfo_database, dbTaskInfo_port, NULL, 0)) {
        sprintf(shortBuffer, "%s\n", mysql_error(conn1));
        log_message(0, shortBuffer);
        daemonExit(1);
    }
    if (!mysql_real_connect(conn2, dbTaskInfo_host, dbTaskInfo_user, dbTaskInfo_password, dbTaskInfo_database, dbTaskInfo_port, NULL, 0)) {
        sprintf(shortBuffer, "%s\n", mysql_error(conn2));
        log_message(0, shortBuffer);
        daemonExit(1);
    }
    if (!mysql_real_connect(conn_dict, db_bim_rap_result_db_host, db_bim_rap_result_db_user, db_bim_rap_result_db_password, db_bim_rap_result_db_database, db_bim_rap_result_db_port, NULL, 0)) {
        sprintf(shortBuffer, "%s\n", mysql_error(conn_dict));
        log_message(0, shortBuffer);
        daemonExit(1);
    }
    log_message(0, "All initialization done.");

    // main loop
    while(1){
        // check for finished tasks
        sprintf(shortBuffer, "check for finished tasks, total: %d",  runningTask.len);
        log_message(0, shortBuffer);
        for(i = 0; i < runningTask.len; i++){
            int status = checkStatus(i);
            sprintf(shortBuffer,"status[%d] is %d", i, status);
            log_message(0, shortBuffer);
            if((status == TSTAT_DONE) || (status == TSTAT_FAIL)){
                // update database
                sprintf(shortBuffer, "update task set status = %d, update_datetime= now() where id = %d", status, runningTask.id[i]);
                if (mysql_query(conn1, shortBuffer)) {
                    sprintf(shortBuffer, "%s\n", mysql_error(conn1));
                    log_message(0, shortBuffer);
                    daemonExit(1);
                }
                // free memory
                deleteTaskInfo(i);
                i --;
            }
        }

        // check tasks to kill
        sprintf(shortBuffer, "select id from task where is_deleted = 0 and status = %d and system_id = 2 and module_id in (3, 4, 5, 6, 7, 9, 10, 11, 12) order by id", TSTAT_TOKILL);
        if (mysql_query(conn1, shortBuffer)) {
            sprintf(shortBuffer, "%s\n", mysql_error(conn1));
            log_message(0, shortBuffer);
            daemonExit(1);
        }
        res = mysql_use_result(conn1);
        while ((row = mysql_fetch_row(res)) != NULL){
            sprintf(shortBuffer,"kill task: %s", row[0]);
            log_message(0, shortBuffer);
            killTask(atoi(row[0]), conn2);
        }
        mysql_free_result(res);
        
        // add running task
        if(runningTask.len == 0)
        {   
            strcpy(shortBuffer, "select id, module_id from task where is_deleted = 0 and status = 3 and module_id in (3, 4, 5, 6, 7, 9, 10, 11, 12) order by id");
            log_message(0, mediumBuffer);
            if (mysql_query(conn1, shortBuffer)) {
                sprintf(shortBuffer, "%s\n", mysql_error(conn1));
                log_message(0, shortBuffer);
                daemonExit(1);
            }    
            res = mysql_use_result(conn1);
            strcpy(mediumBuffer, "add running task");
            log_message(0, mediumBuffer);
            while ((row = mysql_fetch_row(res)) != NULL){
                
                if(runningTask.len < concurrencyLimit){
                    if(atoi(row[1]) == 3){
                        addTask3ToRunningTask(atoi(row[0]),conn2); 
                    }
                    else{
                        struct taskList** subtask = &(runningTask.list[runningTask.len]);
                        runningTask.id[runningTask.len] = atoi(row[0]);
                        runningTask.len ++;
                        (*subtask) = (struct taskList*) malloc(sizeof(struct taskList));
                        (*subtask)->next = NULL;
                        sprintf((*subtask)->taskDir, "%s/task_%d", workspace, atoi(row[0]));
                        subtask = &((*subtask)->next);
                        struct taskList *x;
                        for(x = runningTask.list[runningTask.len - 1]; x; x = x->next){
                            log_message(0, x->taskDir);
                        } 
                    }
                }
                else{
                    sprintf(shortBuffer, "update task set status = %d, update_datetime= now() where id = %s", TSTAT_WAIT, row[0]);
                    log_message(0, shortBuffer);
                    if (mysql_query(conn2, shortBuffer)) {
                        sprintf(shortBuffer, "%s\n", mysql_error(conn1));
                        log_message(0, shortBuffer);
                        daemonExit(1);
                    }
                }    
            }
            mysql_free_result(res);
        }
 
        // check for tasks to be scheduled
        log_message(0, "check for tasks to be scheduled");
        sprintf(shortBuffer, "select id, result_types, module_id from task where is_deleted = 0 and status in (%d, %d) and system_id = 2 and module_id in (3, 4, 5, 6, 7, 9, 10, 11, 12) order by insert_datetime", TSTAT_NEW, TSTAT_WAIT);
        if (mysql_query(conn1, shortBuffer)) {
            sprintf(shortBuffer, "%s\n", mysql_error(conn1));
            log_message(0, shortBuffer);
            daemonExit(1);
        }
        res = mysql_use_result(conn1);
        while ((row = mysql_fetch_row(res)) != NULL){
            sprintf(shortBuffer,"new task: %s\t%s", row[0], row[1]);
            log_message(0, shortBuffer);
            if(runningTask.len < concurrencyLimit){
                if(atoi(row[2]) == 3){
                    log_message(0, "doTask3");
                    doTask3(atoi(row[0]), row[1], conn2, templateDir);
                }
                else if(atoi(row[2]) == 4){
                    log_message(0, "doTask4");
                    doTask4(atoi(row[0]), row[1], conn2, templateDir);
                }
                else if(atoi(row[2]) == 5 || atoi(row[2]) == 6){
                    log_message(0, "doTask5(or 6)");
                    doTask5(atoi(row[0]), row[1], conn2, conn_dict, templateDir);
                }
                else if(atoi(row[2]) == 7){
                    log_message(0, "doTask7");
                    doTask7(atoi(row[0]), row[1], conn2, conn_dict, templateDir);
                }
                else if(atoi(row[2]) == 9){
                    log_message(0, "doTask9");
                    doTask9(atoi(row[0]), row[1], conn2, templateDir);
                }
                else if(atoi(row[2]) == 10){
                    log_message(0, "doTask10");
                    doTask10(atoi(row[0]), row[1], conn2, templateDir);
                }
                else if(atoi(row[2]) == 11){
                    log_message(0, "doTask11");   
                    doTask11(atoi(row[0]), row[1], conn2, templateDir);
                }
                else if(atoi(row[2]) == 12){
                    log_message(0, "doTask12");   
                    doTask12(atoi(row[0]), row[1], conn2, templateDir);
                }
            }
            else{
                sprintf(shortBuffer, "update task set status = %d, update_datetime= now() where id = %s", TSTAT_WAIT, row[0]);
                log_message(0, shortBuffer);
                if (mysql_query(conn2, shortBuffer)) {
                    sprintf(shortBuffer, "%s\n", mysql_error(conn1));
                    log_message(0, shortBuffer);
                    daemonExit(1);
                }
            }
        }
        mysql_free_result(res);
        sleep(5);
    }

    return 0;
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
