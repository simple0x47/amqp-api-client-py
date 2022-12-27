#!/usr/bin/env python
import uuid
import os
import asyncio
import json

from aio_pika import Message, connect
from aio_pika.abc import AbstractIncomingMessage, AbstractQueue
from typing import MutableMapping

AMQP_API_CONNECTION_URI_ENV = 'AMQP_API_CONNECTION_URI'


class AmqpOutputApi(object):
    def __init__(self, response_amqp_config):
        self.connection = None
        self.channel = None
        self.response_queue = None
        self.amqp_uri = os.environ.get(AMQP_API_CONNECTION_URI_ENV)
        self.response_amqp_config = response_amqp_config
        self.loop = asyncio.get_running_loop()
        self.latest_result: asyncio.Future = None

    async def connect(self):
        self.connection = await connect(self.amqp_uri, loop=self.loop)

        self.channel = await self.connection.channel()
        await self.channel.set_qos(self.response_amqp_config["channel"]["qos"]["prefetch_count"],
                                   self.response_amqp_config["channel"]["qos"]["prefetch_size"],
                                   self.response_amqp_config["channel"]["qos"]["global"],
                                   self.response_amqp_config["channel"]["qos"]["timeout"],
                                   self.response_amqp_config["channel"]["qos"]["all_channels"])

        self.response_queue = await self.channel.declare_queue(self.response_amqp_config["queue"]["name"],
                                                               durable=self.response_amqp_config["queue"]["durable"],
                                                               exclusive=self.response_amqp_config["queue"]["exclusive"],
                                                               passive=self.response_amqp_config["queue"]["passive"],
                                                               auto_delete=self.response_amqp_config["queue"]["auto_delete"],
                                                               arguments=self.response_amqp_config["queue"]["arguments"])

        return self

    def on_response(self, message: AbstractIncomingMessage):
        if self.latest_result is None:
            return

        self.latest_result.set_result(message.body)

    async def read(self):
        self.latest_result = self.loop.create_future()

        await self.response_queue.consume(self.on_response,
                                          self.response_amqp_config["channel"]["consume"]["no_ack"],
                                          self.response_amqp_config["channel"]["consume"]["exclusive"],
                                          self.response_amqp_config["channel"]["consume"]["arguments"],
                                          self.response_amqp_config["channel"]["consume"]["consumer_tag"],
                                          self.response_amqp_config["channel"]["consume"]["timeout"])

        return await self.latest_result
