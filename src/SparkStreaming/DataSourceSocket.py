# python3
# -*- coding:utf-8 -*-
# @Time: 9/5/19 11:22 AM
# @Author: Damon
# @Software: PyCharm

'''
编程实现Socket服务端作为自定义数据源
'''

import socket

#生成socket对象
server=socket.socket()
#绑定ip和端口
server.bind(("localhost",9999))
#监听绑定的端口
server.listen(1)
while 1:
    print("我在等待客户端连接请求......")
    #服务端进入阻塞状态，conn是连接对象，addr【0】：ip地址，addr[1]:端口号
    conn,addr=server.accept()
    print("连接地址 %s 成功..." %addr[0])
    print("发送数据......")
    conn.send("我支持香港警察，我爱中国......".encode())
    conn.close()
    print("连接关闭......")
