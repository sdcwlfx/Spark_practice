# python3
# -*- coding:utf-8 -*-
# @Time: 9/9/19 11:29 AM
# @Author: Damon
# @Software: PyCharm

from kafka import KafkaProducer
import json
import time

#时间戳转制定格式日期
def timestamp_to_date(timestamp):
    timeArray=time.localtime(timestamp)
    otherStyleTime=time.strftime("%Y--%m--%d %H:%M:%S",timeArray)
    return otherStyleTime


if __name__ == "__main__":
    # 生产者对象
    producer=KafkaProducer(
        value_serializer=lambda v:json.dumps(v).encode("utf-8"),
        bootstrap_servers=["localhost:9092"]
    )
    i=0
    while True:
        #将float型时间戳转为整型
        time_stamp=int(time.time())
        print(timestamp_to_date(time_stamp))
        #JSON数据
        data = {
            "id": i,
            "mac": "44-C3-49-5A-E5-93",
            "brand": "Huawei Technologies Co.Ltd",
            "cache_ssid": "",
            "capture_time": time_stamp,
            "terminal_fieldstrenth": -5,
            "identification_type": "",
            "certificate_code": "",
            "ssid_position": "",
            "access_ap_mac": "68-DB-54-E0-A0-62",
            "access_ap_channel": "",
            "access_ap_encryption_type": "99",
            "x_coordinate": 0.0,
            "y_coordinate": 0.0,
            "netbar_wacode": "",
            "collection_equipmentid": "210102000000181",
            "collection_equipment_longitude": 123.418713,
            "collection_equipment_latitude": 41.7719617,
            "lng_lat": "124.418713::41.7719617"
        }
        #value = bytearray(data, 'utf-8')
        # 发送数据到指定topic，超时时间10秒
        producer.send('big_data_kj', value=data).get(timeout=10)
        #每0.1秒发送由两个英文字母组成的字符串
        time.sleep(30)
        i += 1

    producer.close()