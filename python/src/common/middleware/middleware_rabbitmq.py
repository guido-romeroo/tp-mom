import pika
import random
import string
from .middleware import MessageMiddlewareCloseError, MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError, MessageMiddlewareQueue, MessageMiddlewareExchange
from pika.exceptions import AMQPChannelError, AMQPConnectionError

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()

        self.queue_name = queue_name
        self.channel.queue_declare(queue=queue_name, durable=True)

    def close(self):
        try:
            self.connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError() from e

    def send(self, message):
        self.validate_connection()
        try:
            self.channel.basic_publish(exchange='', routing_key=self.queue_name, body=message, properties=pika.BasicProperties(delivery_mode=2)) # delivery_mode=2 para persistir los mensajes
        except (AMQPConnectionError, AMQPChannelError) as e:
            raise MessageMiddlewareDisconnectedError() from e
        except Exception as e:
            raise MessageMiddlewareMessageError() from e

        
    def start_consuming(self, on_message_callback):
        def callback(ch, method, properties, body):
            on_message_callback(body, lambda: ch.basic_ack(delivery_tag=method.delivery_tag), lambda: ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True))

        self.channel.basic_qos(prefetch_count=1) 
        self.channel.basic_consume(queue=self.queue_name, on_message_callback=callback) 
        try:
            self.channel.start_consuming()
        except (AMQPConnectionError, AMQPChannelError):
            raise MessageMiddlewareDisconnectedError()
        except Exception as e:
            raise MessageMiddlewareMessageError() from e

    def stop_consuming(self):
        self.validate_connection()
        self.channel.stop_consuming()

    def validate_connection(self):
        if self.channel.is_closed or self.connection.is_closed:
            raise MessageMiddlewareDisconnectedError()

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        pass
