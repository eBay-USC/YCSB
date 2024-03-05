#!/usr/bin/env python3
import subprocess
import time

databases = ["foundationdb", "memcached"]
storages = ["sqlite","redis","memcached","memory"]
workloads = ["a","b","c","d","e","f"]
readversion = ["new","old"]
key_location="/users/dzhdhr/.ssh/id_ed25519"
path_to_fdb = "/users/gyming/fdb/"
number_threads = [100]
summary = {}
# Stop all no stop fdb monister process in prev run
subprocess.call("ssh dzhdhr@apt070.apt.emulab.net \"sudo sh /users/gyming/fdb/stop.sh\"",shell=True)
# Start running
for n in number_threads:
    for wl in workloads:
        for database in databases:
            #run fdb benchmark
            if database == "foundationdb":
                for storage in storages:
                    out_file = f"out/thread_{n}/fresh/{database}/{storage}/use_cache/{wl}"
                    print(f"###### Running workload{wl} with database {database}+ {storage} with cache on {n}thread ###")
                    subprocess.call(f"mkdir -p {out_file}",shell=True)
                    starttime = time.time()
                    if storage =="memory":
                        # boot up fdb server
                        subprocess.call(f"ssh dzhdhr@apt070.apt.emulab.net \"sudo sh /{path_to_fdb}/redis/run.sh\"",shell=True)
                        print(f"start runing /{path_to_fdb}/redis/run.sh")
                        time.sleep(10)
                        # change fdb to meomry mode
                        subprocess.call("fdbcli --exec \"writemode on; configure memory\"",shell=True)
                        # load in data
                        subprocess.call(f"./bin/ycsb load {database} -s -P workloads/{database}/thread_{n}/workload{wl} -p \"foundationdb.subspace=normal\">{out_file}/out_load.txt",shell=True) 
                        print(f"###### Finish loading workload{wl} with database {database}+ {storage} with cache on {n}thread ###")
                        time.sleep(10)
                        # run benchmark
                        subprocess.call(f"./bin/ycsb run {database} -s -threads {n} -P  workloads/{database}/thread_{n}/workload{wl} -p \"foundationdb.subspace=normal\">{out_file}/out_run.txt",shell=True)
                    elif storage =="sqlite":
                        # boot up fdb server
                        subprocess.call(f"ssh dzhdhr@apt070.apt.emulab.net \"sudo sh /{path_to_fdb}/redis/run.sh\"",shell=True)
                        print(f"start runing /{path_to_fdb}/redis/run.sh")
                        time.sleep(10)
                        # change fdb to sqlite mode
                        subprocess.call("fdbcli --exec \"writemode on; configure ssd\"",shell=True)
                        # load in dataset
                        subprocess.call(f"./bin/ycsb load {database} -s -P workloads/{database}/thread_{n}/workload{wl} -p \"foundationdb.subspace=normal\" >{out_file}/out_load.txt",shell=True)
                        print(f"###### Finish loading workload{wl} with database {database}+ {storage} with cache on {n}thread ###")
                        time.sleep(10)
                        # start benchmark
                        subprocess.call(f"./bin/ycsb run {database} -s -threads {n} -P workloads/{database}/thread_{n}/workload{wl} -p \"foundationdb.subspace=normal\">{out_file}/out_run.txt",shell=True)
                    else:
                        # boot up fdb server
                        subprocess.call(f"ssh dzhdhr@apt070.apt.emulab.net \"sudo sh /{path_to_fdb}/{storage}/run.sh\"",shell=True)
                        print(f"start runing /{path_to_fdb}/{storage}/run.sh")
                        time.sleep(10)
                        # load in dataset
                        subprocess.call(f"./bin/ycsb load {database} -s -P workloads/{database}/thread_{n}/workload{wl} >{out_file}/out_load.txt",shell=True)
                        print(f"###### Finish loading workload{wl} with database {database}+ {storage} with cache on {n}thread ###")
                        time.sleep(10)
                        # start benchmark
                        subprocess.call(f"./bin/ycsb run {database} -s -threads {n} -P workloads/{database}/thread_{n}/workload{wl} >{out_file}/out_run.txt",shell=True)
                    # clear database
                    subprocess.call("fdbcli --exec \"writemode on; clearrange 0x00 0xff\"",shell=True)
                    # stop f-monister process
                    subprocess.call("ssh dzhdhr@apt070.apt.emulab.net \"sudo sh /users/gyming/fdb/stop.sh\"",shell=True)
                    endTime = time.time()
                    summary[f"{database} cached with {storage} on workload{wl} {n}thread"] = endTime-starttime
                    print(f"###### Finish work{wl} with database {database}with {storage} with cache")


            else:
                pass
# running no cache benchmark
for n in number_threads:
    for wl in workloads:
        for database in databases:
            #run fdb benchmark
            if database == "foundationdb":
                for storage in ["sqlite"]:
                    out_file = f"out/thread_{n}/fresh/{database}/{storage}/no_cache/{wl}"
                    print(f"###### Running workload{wl} with database {database}+ {storage} without cache on {n}thread #####")
                    starttime = time.time()
                    subprocess.call(f"mkdir -p {out_file}",shell=True)
                    if storage =="memory":
                        # boot up fdb server
                        subprocess.call(f"ssh dzhdhr@apt070.apt.emulab.net \"sudo sh /{path_to_fdb}/redis/run_cache.sh\"",shell=True)
                        print(f"start runing /{path_to_fdb}/redis/run_cache.sh")
                        time.sleep(10)
                        # change fdb to meomry mode
                        subprocess.call("fdbcli --exec \"writemode on; configure memory\"",shell=True)
                        # load in dataset
                        subprocess.call(f"./bin/ycsb load {database} -s  -P workloads/{database}/thread_{n}/workload{wl} -p \"foundationdb.subspace=normal\">{out_file}/out_load.txt",shell=True)
                        print(f"###### Finish loading workload{wl} with database {database}+ {storage} with without on {n}thread ###")
                        time.sleep(10)
                        # run benchmark
                        subprocess.call(f"./bin/ycsb run {database} -s -threads {n} -P  workloads/{database}/thread_{n}/workload{wl} -p \"foundationdb.subspace=normal\">{out_file}/out_run.txt",shell=True)
                    elif storage =="sqlite":
                        # boot up fdb server
                        subprocess.call(f"ssh dzhdhr@apt070.apt.emulab.net \"sudo sh /{path_to_fdb}/redis/run_cache.sh\"",shell=True)
                        print(f"start runing /{path_to_fdb}/redis/run_cache.sh")
                        time.sleep(10)
                        # change fdb to sqlite mode
                        subprocess.call("fdbcli --exec \"writemode on; configure ssd\"",shell=True)
                        # load in dataset
                        subprocess.call(f"./bin/ycsb load {database} -s -P workloads/{database}/thread_{n}/workload{wl} -p \"foundationdb.subspace=normal\" >{out_file}/out_load.txt",shell=True)
                        print(f"###### Finish loading workload{wl} with database {database}+ {storage} without cache on {n}thread ###")
                        time.sleep(10)
                        # run benchmark
                        subprocess.call(f"./bin/ycsb run {database} -s -threads {n} -P workloads/{database}/thread_{n}/workload{wl} -p \"foundationdb.subspace=normal\">{out_file}/out_run.txt",shell=True)
                    else:
                        # boot up fdb server
                        subprocess.call(f"ssh dzhdhr@apt070.apt.emulab.net \"sudo sh /{path_to_fdb}/{storage}/run_cache.sh\"",shell=True)
                        print(f"start runing /{path_to_fdb}/{storage}/run_cache.sh")
                        time.sleep(10)
                         # load in dataset
                        subprocess.call(f"./bin/ycsb load {database} -s -P workloads/{database}/thread_{n}/workload{wl} >{out_file}/out_load.txt",shell=True)
                        print(f"###### Finish loading workload{wl} with database {database}+ {storage} without cache on {n}thread ###")
                        time.sleep(10)
                        # run benchmark
                        subprocess.call(f"./bin/ycsb run {database} -s -threads {n} -P workloads/{database}/thread_{n}/workload{wl} >{out_file}/out_run.txt",shell=True)
                    subprocess.call("fdbcli --exec \"writemode on; clearrange 0x00 0xff\"",shell=True)
                    subprocess.call("ssh dzhdhr@apt070.apt.emulab.net \"sudo sh /users/gyming/fdb/stop.sh\"",shell=True)
                    endTime = time.time()
                    summary[f"{database} no cached with {storage} on workload{wl} {n}thread"] = endTime-starttime
                    print(f"###### Finish work{wl} with database {database}with {storage} without cache on {n}thread")
            else:
                pass
# # print summary
print("Summary:")
for key, value in summary.items():
    print(f"{key} take, {value}s")