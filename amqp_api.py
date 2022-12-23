#!/usr/bin/env python
import uuid
import os
import asyncio
import json

from aio_pika import Message, connect
from aio_pika.abc import AbstractIncomingMessage, AbstractQueue
from typing import MutableMapping

AMQP_API_CONNECTION_URI_ENV = 'AMQP_API_CONNECTION_URI'


class AmqpApi(object):
    def __init__(self, request_amqp_config, response_amqp_config):
        self.amqp_uri = os.environ.get(AMQP_API_CONNECTION_URI_ENV)
        self.request_amqp_config = request_amqp_config
        self.response_amqp_config = response_amqp_config
        self.loop = asyncio.get_running_loop()
        self.futures: MutableMapping[str, asyncio.Future] = {}

    async def connect(self):
        self.connection = await connect(self.amqp_uri, loop=self.loop)

        self.channel = await self.connection.channel()

        self.request_queue = await self.channel.declare_queue(self.request_amqp_config["queue"]["name"],
                                                              durable=self.request_amqp_config["queue"]["durable"],
                                                              exclusive=self.request_amqp_config["queue"]["exclusive"],
                                                              passive=self.request_amqp_config["queue"]["passive"],
                                                              auto_delete=self.request_amqp_config["queue"]["auto_delete"],
                                                              arguments=self.request_amqp_config["queue"]["arguments"])

        self.response_queue = await self.channel.declare_queue(self.response_amqp_config["queue"]["name"],
                                                               durable=self.response_amqp_config["queue"]["durable"],
                                                               exclusive=self.response_amqp_config["queue"]["exclusive"],
                                                               passive=self.response_amqp_config["queue"]["passive"],
                                                               auto_delete=self.response_amqp_config["queue"]["auto_delete"],
                                                               arguments=self.response_amqp_config["queue"]["arguments"])

        await self.response_queue.consume(self.on_response)

        return self

    def on_response(self, message: AbstractIncomingMessage):
        if message.correlation_id is None:
            return

        if message.correlation_id not in self.futures.keys():
            return

        future: asyncio.Future = self.futures.pop(message.correlation_id)
        future.set_result(message.body)

    async def send_request(self, request):
        correlation_id = str(uuid.uuid4())
        future = self.loop.create_future()

        self.futures[correlation_id] = future

        request_message = Message(json.dumps(request).encode('utf-8'),
                                  content_type="application/json",
                                  correlation_id=correlation_id,
                                  reply_to=self.response_queue.name)

        await self.channel.default_exchange.publish(request_message, routing_key=self.request_queue.name)
        return await future
