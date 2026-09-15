import pika
import random
import string
from .middleware import MessageMiddlewareCloseError, MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError, MessageMiddlewareQueue, MessageMiddlewareExchange
from pika.exceptions import ChannelClosed, UnroutableError, NackError

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()
        self.channel.confirm_delivery() # publisher confirms

        self.queue_name = queue_name
        self.channel.queue_declare(queue=queue_name, durable=True) # queues durables para resistir caídas del broker

    def close(self):
        try:
            self.connection.close()
        except:
            raise MessageMiddlewareCloseError()

    def send(self, message):
        if self.channel.is_closed or self.connection.is_closed:
            raise MessageMiddlewareDisconnectedError()
        try:
            self.channel.basic_publish(exchange='', routing_key=self.queue_name, body=message, properties=pika.BasicProperties(delivery_mode=2)) # delivery_mode=2 para persistir los mensajes
        except Exception as e:
            raise MessageMiddlewareMessageError(e)

        
    def start_consuming(self, on_message_callback):
        def callback(ch, method, properties, body):
            on_message_callback(body, lambda: ch.basic_ack(delivery_tag=method.delivery_tag), lambda: ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True))
            # preguntar en clase si se debe usar requeue en las working queues

        self.channel.basic_consume(queue=self.queue_name, on_message_callback=callback) 

        try:
            self.channel.start_consuming()
        except ChannelClosed:
            raise MessageMiddlewareDisconnectedError()
        except Exception as e:
            raise MessageMiddlewareMessageError(e)

    def stop_consuming(self):
        if self.channel.is_closed or self.connection.is_closed:
            raise MessageMiddlewareDisconnectedError()
        self.channel.stop_consuming()

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        pass
