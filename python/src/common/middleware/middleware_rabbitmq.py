import pika
import random
import string
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange

from .middleware import MessageMiddlewareMessageError, MessageMiddlewareDisconnectedError, MessageMiddlewareCloseError, MessageMiddlewareDeleteError
import pika.exceptions

def make_callback(on_message_callback):
    def callback(ch, method, properties, body):
        def ack():
            ch.basic_ack(delivery_tag=method.delivery_tag)

        def nack():
            ch.basic_nack(delivery_tag=method.delivery_tag)

        on_message_callback(body, ack, nack)
    return callback

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self.host = host
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(self.host))
        self.queue_name = queue_name
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue_name, durable=True)

    def start_consuming(self, on_message_callback):
        try:
            self.channel.basic_qos(prefetch_count=1)
            self.channel.basic_consume(queue=self.queue_name, on_message_callback=make_callback(on_message_callback))
            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError("Connection to RabbitMQ lost.")
        except Exception as e:
            raise MessageMiddlewareMessageError(str(e))

    def stop_consuming(self):
        try:
            self.channel.stop_consuming()
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError("Connection to RabbitMQ lost.")

    def send(self, message):
        try:
            self.channel.basic_publish(exchange='', routing_key=self.queue_name, body=message)
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError("Connection to RabbitMQ lost.")
        except Exception as e:
            raise MessageMiddlewareMessageError(str(e))

    def close(self):
        try:
            self.channel.close()
            self.connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError(str(e))

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        self.host = host
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=self.exchange_name, exchange_type='direct', durable=True)

    def start_consuming(self, on_message_callback):
        try:
            self.channel.basic_qos(prefetch_count=1)
            result = self.channel.queue_declare(queue='', exclusive=True)
            queue_name = result.method.queue

            for routing_key in self.routing_keys:
                self.channel.queue_bind(exchange=self.exchange_name, queue=queue_name, routing_key=routing_key)

            self.channel.basic_consume(queue=queue_name, on_message_callback=make_callback(on_message_callback))
            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError("Connection to RabbitMQ lost.")
        except Exception as e:
            raise MessageMiddlewareMessageError(str(e))

    def stop_consuming(self):
        try:
            self.channel.stop_consuming()
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError("Connection to RabbitMQ lost.")

    def send(self, message):
        try:
            for routing_key in self.routing_keys:
                self.channel.basic_publish(exchange=self.exchange_name, routing_key=routing_key, body=message)
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError("Connection to RabbitMQ lost.")
        except Exception as e:
            raise MessageMiddlewareMessageError(str(e))

    def close(self):
        try:
            self.channel.close()
            self.connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError(str(e))
