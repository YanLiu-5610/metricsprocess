# -*- coding:utf-8 -*-
import json
import time
import os
from multiprocessing import Pool
import cachetools
import shutil
import pymysql
import prometheus_client
from prometheus_client import Gauge
from prometheus_client.core import CollectorRegistry
import requests

# 注册
registry = CollectorRegistry(auto_describe=False)

#定义系统名称
system_name = ["'RCS-KM'",]
# 1. 连接数据库，
conn = pymysql.connect(
    host='10.108.134.242',
    user='pros_monitor',
    password='rpddb@1234.',
    port=5000,
    db='pros_monitor',
    charset='utf8',
)
cur = conn.cursor()
system_all_ip = []
system_ip = []
instance_ip = {}
systemIds = []
for system in system_name:
    system_ip = []
    sql = "SELECT b.subsys_name,a.subsys, a.ip FROM devops_subsystem_ip_list a  inner join devops_subsystem_list  b  ON a.subsys = b.subsys_id  WHERE b.subsys_name=" + system + " AND a.isDelete=0"
    cur.execute(sql)
    data=cur.fetchall()
    systemId = data[1][1]
    for item in data:
        system_ip.append(item[2])
    instance_ip[systemId] = system_ip
    systemIds.append(systemId)
cur.close()



#1s清空内存
ROTATE = 5
@cachetools.cached(cachetools.TTLCache(1, ROTATE))
def reload(item):
    with open("../datafile/" + item + "_metricsvalue.txt") as f:
        parameters = f.readlines()
    return parameters

class Model():
    def log(self):
        for item in systemIds:
            self.model = reload(item)
            system = 'metric_' + item
            g = Gauge(system, 'metricId', ['instance', 'metricname', 'metricId', 'system'], registry=registry)
            for part in self.model[1:]:
                metric_name = "_".join(part.split()[0].split('_')[:-2])
                print(metric_name)
                id = part.split()[0].split('_')[-2]
                ip = part.split()[0].split('_')[-1]
                value = part.split()[1].strip('\n')
                system_id = item
                metric_name = metric_name.split(']')[1]
                print(metric_name)
                g.labels(instance=ip,metricname=metric_name,metricId=id,system=system_id).set(value)
            requests.post("http://192.168.91.132:9091/metrics/job/network_traffic",
                      data=prometheus_client.generate_latest(registry))

def sub_process(system_all,meId_combine_inst,metricId,HostIp,now_milli_time):
    metrics_value_file = open("../datafile/" + system_all + "_metricsvalue.txt", "a")
    # 获取所有指标值
    Metric_value_url = 'http://' + HostIp + '/ims_config/getMetricDataByStamp.do?userAuthKey=sit_test&metricIds='+ metricId +'&startTimestamp=' + now_milli_time + '&endTimestamp=' + now_milli_time
    Metric_value_result = requests.get(Metric_value_url)
    Metric_value_result = json.loads(Metric_value_result.text)
    Metric_value_result = Metric_value_result['metricDataResultMap']
    for metric in Metric_value_result:
        for meId in meId_combine_inst:
            if metric == meId.split(',')[0]:
                line = Metric_value_result[metric]['metricName'] +'_'+ metric + '_' + meId.split(',')[1] + ' ' + str(Metric_value_result[metric]['valueList'][0])
                metrics_value_file.write(line)
                metrics_value_file.write('\n')
    metrics_value_file.close()

def father_process(instance_ip):

    # 定义变量
    HostIp = '10.107.119.24:10815'

    metricIds = []
    now_milli_time = int(time.time() * 1000)
    now_milli_time = str(now_milli_time)

    system_meId_inst = {}
    system_all_meId = {}
    meId_combine_inst = []
    metricid_all = []
    instance_ip = {'1055':['172.21.172.44', '172.21.36.67', '172.21.35.212', '172.21.65.18', '172.21.42.132', '172.21.64.7', '172.21.66.2', '172.21.66.3'],
                   '1023':['172.21.172.44', '172.21.36.67', '172.21.35.212', '172.21.65.18', '172.21.42.132', '172.21.64.7', '172.21.66.2', '172.21.66.3']}
    for subSystemIds in instance_ip:
        for ip in instance_ip[subSystemIds]:
            url = 'http://' + HostIp + '/ims_config/getMetricIds.do?userAuthKey=sit_test&subSystemIds=' + subSystemIds + '&objectName=' + ip + '&monitorType=4'
            Metric_name_result = requests.get(url)
            Metric_name_result = json.loads(Metric_name_result.text)
            Metric_name_result = Metric_name_result['retData']
            Metric_name_result = ["108027934", "108028031", "108028034"]
            # system_name = system_name.replace('-', '_').strip("'")
            for id in Metric_name_result:
                metricid_all.append(id)
                meId_combine_inst.append(id + ',' + ip)
        system_all_meId[subSystemIds] = metricid_all
        system_meId_inst[subSystemIds] = meId_combine_inst
        for system in system_all_meId:
            system_all_meId[system] = [system_all_meId[system][i:i + 6] for i in range(0, len(system_all_meId[system]), 6)]
        for system in system_all_meId:
            for part in system_all_meId[system]:
                metricId = ''
                for inx, val in enumerate(part):
                    if inx == len(part)-1:
                        metricId += val
                    else:
                        metricId += val + ','
                metricIds.append(metricId)
            system_all_meId[system] = metricIds
    for name in systemIds:
        if os.path.exists("../datafile/" + name + "_metricsvalue.txt"):
            shutil.copy("../datafile/" + name + "_metricsvalue.txt","../datafile/" + name + "_metricsvalue_backup.txt")
            os.remove("../datafile/" + name + "_metricsvalue.txt")
    #指定同时运行的进程数
    P=Pool(processes=4)
    for system_all in system_all_meId:
            for id in system_all_meId[system_all]:
                P.apply_async( func = sub_process,
            args = (system_all,system_meId_inst[system_all],id,HostIp,now_milli_time ))
        # 不能继续添加进程了
    P.close()
    P.join()


def main():
    model = Model()
    father_process(instance_ip)
    model.log()

if __name__ == "__main__":
    main()

