import argparse
import dataclasses
import json
from dataclasses import dataclass
import socket
import logging
logging.basicConfig(level=logging.DEBUG)

# 序列化方式为最简单的转为二进制

KAFKA_IP = "123.57.178.10"
KAFKA_PORT = "9090"


@dataclass
class Mesaage(object):
    Id: str  # 学号
    Ip: str  # 你的IP
    Status: str
    content: str  # 问题答案
    uuid: str  # 初始为空


class IProducer(object):
    def Produce(self, topic: str, ms: Mesaage):
        raise NotImplementedError
    def Connect(self, ip: str, port: str):
        raise NotImplementedError


class Producer(IProducer):

    @dataclass
    class Request(object):
        Topic: str
        Ms: Mesaage

    Ip: str
    Port: str
    Rq: Request

    def Produce(self, topic: str, ms: Mesaage):
        self.Rq = Producer.Request(Topic=topic, Ms=ms)
        to_send = {
            'Ip': self.Ip,
            'Port': self.Port,
            'Request': dataclasses.asdict(self.Rq)
        }
        bin: bytes = json.dumps(to_send).encode('utf-8')
        sent_bytes = self._socket.send(bin);logging.info(f'Sent {sent_bytes} bytes')

    def Connect(self, ip: str, port: str):
        # TCP
        self.Ip = ip
        self.Port = port
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.connect((self.Ip, int(self.Port)))

class IConsumer(object):
    def Consume(self, topic: str, ms: Mesaage):
        raise NotImplementedError
    def Connect(self, ip: str, port: str):
        raise NotImplementedError

class Consumer(IConsumer):
    class Request(object):
        Id: str
        Name: str
        Topic: str
        Ms: Mesaage

    Ip: str
    Port: str
    Resp: bytes # 不知道会回复什么

    def Connect(self, ip: str, port: str):
        # TCP
        self.Ip = ip
        self.Port = port
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.connect((self.Ip, int(self.Port)))

    def Consume(self, topic: str, ms: Mesaage):
        recved_bin = self._socket.recv(1024)  # 1024bytes，应该足够大了


###################################
# 我瞎写的
EXAMPLE_PRODUCED_MSG = Mesaage(Id="200000000", Ip="1.11.111.1", Status="", content="test", uuid="")
EXAMPLE_TOPIC = "homework1"


def producer_main():
    producer = Producer()
    producer.Connect(KAFKA_IP, KAFKA_PORT)
    producer.Produce(EXAMPLE_TOPIC, EXAMPLE_PRODUCED_MSG)
    producer._socket.close()


def consumer_main():
    consumer = Consumer()
    consumer.Connect(KAFKA_IP, KAFKA_PORT)
    recved = consumer._socket.recv(1024)
    consumer._socket.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--role', choices=('producer', 'consumer'), type=str)
    args = parser.parse_args()

    main = vars()[args.role + '_main']
    main()
