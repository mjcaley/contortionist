#!/usr/bin/env python3

from aiosmtpd.controller import Controller


class ExampleHandler:
    async def handle_RCPT(self, server, session, envelope, address, rcpt_options):
        if not address.endswith('@example.com'):
            return '550 not relaying to that domain'
        envelope.rcpt_tos.append(address)
        return '250 OK'

    async def handle_DATA(self, server, session, envelope):
        print('Message from {}'.format(envelope.mail_from))
        print('Message for {}'.format(envelope.rcpt_tos))
        print('Message data:\n')
        print(envelope.content.decode('utf8', errors='replace'))
        print('End of message')
        return '250 Message accepted for delivery'


controller = Controller(ExampleHandler())
print('Starting SMTPD')
controller.start()

from smtplib import SMTP as Client
client = Client(controller.hostname, controller.port)
r = client.sendmail('sender@example.com', ['receiver@example.com'],
                    'This is a test message')

print('Stopping SMTPD')
controller.stop()
