#!/bin/env python
#coding:utf-8

## 
## @file extractTemplate.py
## @author zhouyuefeng
## @date 2014/06/14 22:01:09
## @brief 
##  
## 

import sys;
import getopt;
import json;
import os;
import shutil;

def recursiveCopy(src, dst):
    if os.path.isdir(src):
        if not os.path.exists(dst):
            os.makedirs(dst)
        for item in os.listdir(src):
            s = os.path.join(src, item)
            d = os.path.join(dst, item)
            if os.path.isdir(s):
                recursiveCopy(s, d)
            else:
                if not os.path.exists(d):
                    shutil.copy2(s, d)
    elif not os.path.exists(dest):
        shutil.copy2(src, dest);

def help(out = -1):
    if out == 0:
        output = sys.stdout;
    else:
        output = sys.stderr;
    print >> output, "  USAGE:\n\
    help information here.\n\
";

def file_get_contents(file):
    with open(file, 'r') as f:
        return f.read();

def file_put_contents(file, contents):
    with open(file, 'w') as f:
        f.write(contents);

if __name__ == "__main__":
    try:
        options,args = getopt.getopt(sys.argv[1:],"hc:t:w:v:s:",["help","config=","tar=","workspace=","variable=","jobset="]);
    except getopt.GetoptError:
        help();
        exit(1);

    if len(args) > 0:
        help();
        exit(1);

    configFile = [];
    target = [];
    templateDir = "template";
    workspace = "workspace/default";
    variables = [];
    jobSets = [];
    for name, value in options:
        if name in ("-h", "--help"):
            help(0);
            exit(0);
        if name in ("-c", "--config"):
            configFile.append(value);
            continue;
        if name in ("-t", "--tar"):
            target.append(value);
            continue;
        if name in ("-w", "--workspace"):
            workspace = value;
            continue;
        if name in ("-v", "--variable"):
            variables.append(value);
            continue;
        if name in ("-s", "--jobset"):
            jobSets.append(value);
            continue;

    if len(configFile) == 0:
        configFile = ['template/conf/default.conf'];

    if len(target) == 0:
        target = ['tail'];

    stra_name = workspace.split(os.sep)[-1];

    tmpDir = os.path.join(workspace, ".extractTemplateTemp");

    
    jobs = [];
    for file in configFile:
        json_str = file_get_contents(file);
        jobs.extend(json.JSONDecoder().decode(json_str));

    uniqJobs = {};
    for job in jobs:
        uniqJobs[job['job']] = job;


    analysedJobs = [];
    analysingJobs = target;
    directories = [];
    files = {};

    for jobSetString in jobSets:
        job, dependency = jobSetString.split('=');
        dependency = dependency.split(',');
        uniqJobs[job]={'job':job, 'dependency':dependency};

    while len(analysingJobs) > 0:
        job = analysingJobs.pop();
        analysedJobs.append(job);
        job = uniqJobs[job];
        if 'dependency' in job:
            for dependantJob in job['dependency']:
                if dependantJob not in analysedJobs and dependantJob not in analysingJobs:
                    analysingJobs.append(dependantJob);
        if 'directory' in job:
            for dir in job['directory']:
                if dir not in directories:
                    directories.append(dir);
        if 'file' in job:
            for file in job['file']:
                if file not in files:
                    files[file] = [];
                for d in job['file'][file]:
                    if d not in files[file]:
                        files[file].append(d);

    if not os.path.isdir(workspace):
        os.makedirs(workspace);

    if not os.path.isdir(tmpDir):
        os.makedirs(tmpDir);

    for dir in directories:
        dir = os.path.join(workspace,dir);
        if not os.path.isdir(dir):
            os.makedirs(dir);

    for file in files:
        src = os.path.join(templateDir, 'file', file);
        for dest in files[file]:
            dest = os.path.join(workspace, dest);
            recursiveCopy(src, dest);

    scriptDir = os.path.join(workspace, "script");

    for job in analysedJobs:
        jobConf = uniqJobs[job];
        if "command" in jobConf:
            cmd = jobConf["command"];
        else:
            cmd = "";
        tmpCmdFile = os.path.join(tmpDir, "tmpCmd");
        file_put_contents(tmpCmdFile, cmd);
        os.system('cat "' + \
                os.path.join(templateDir, "iterHead") + \
                '" "' + tmpCmdFile + \
                '" "' + os.path.join(templateDir, "iterEnd") + \
                '" > "' + os.path.join(scriptDir, job + ".sh") +'"');

    runConf = u"";
    for var in variables:
        runConf += var.decode('utf-8') + "\n";

    runConf += "task_dep_var=${basic_dep_var}";
    for var in variables:
        runConf += "," + var.split("=")[0];
    runConf += "\n";
        
    for job in analysedJobs:
        runConf += job + "_var_list=${task_dep_var}\n";

    runConf += "[iteration]\n";

    for job in analysedJobs:
        jobConf = uniqJobs[job];
        if 'dependency' in jobConf:
            dep = "";
            for d in jobConf['dependency']:
                if dep == "":
                    dep = "${" + d + "}";
                else:
                    dep += ",${" + d + "}";
            runConf += job + "\t0\t1\t${default_timeout}\t" + job + ".sh\t" + dep + "\n";
        else:
            runConf += job + "\t${batch_freq}\t1\t${default_timeout}\t" + job + ".sh\t${null}\n";

    file_put_contents(os.path.join(tmpDir, "run.conf"), runConf.encode('utf-8'));

    os.system('cat "' + \
            os.path.join(templateDir, "run.conf") + \
            '" "' + os.path.join(tmpDir, "run.conf") + \
            '" > "' + os.path.join(workspace, "run.conf") + '"');

    os.system("sed -i -e 's/^stra_name=$/stra_name=" + stra_name + "/' -e 's/stra_control=off/stra_control=on/' " + os.path.join(workspace, "run.conf"));

# vim: set expandtab ts=4 sw=4 sts=4 tw=100:
