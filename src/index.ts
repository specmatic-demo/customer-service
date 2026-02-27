import express, { type Request, type Response } from 'express';
import { randomUUID } from 'node:crypto';
import { Kafka, type Consumer, type EachMessagePayload, type Producer } from 'kafkajs';
import type {
  AnalyticsNotificationEvent,
  Customer,
  CustomerPreferenceSyncReplyEvent,
  CustomerPreferenceSyncRequestEvent,
  CustomerProfileUpdatedEvent
} from './types';

const app = express();
app.use(express.json({ limit: '1mb' }));

const host = process.env.CUSTOMER_HOST || '0.0.0.0';
const port = Number.parseInt(process.env.CUSTOMER_PORT || '9000', 10);
const kafkaBrokers = (process.env.CUSTOMER_KAFKA_BROKERS || 'localhost:5411')
  .split(',')
  .map((value) => value.trim())
  .filter(Boolean);
const profileUpdatedTopic = process.env.CUSTOMER_PROFILE_UPDATED_TOPIC || 'customer.profile.updated';
const preferenceSyncRequestTopic = process.env.CUSTOMER_PREFERENCE_SYNC_REQUEST_TOPIC || 'customer.preference.sync.request';
const preferenceSyncReplyTopic = process.env.CUSTOMER_PREFERENCE_SYNC_REPLY_TOPIC || 'customer.preference.sync.reply';
const preferenceSyncConsumerGroup = process.env.CUSTOMER_PREFERENCE_SYNC_GROUP || 'customer-service-preference-sync';
const analyticsNotificationTopic = process.env.ANALYTICS_NOTIFICATION_TOPIC || 'notification.user';

const customerStore = new Map<string, Customer>();
const kafka = new Kafka({
  clientId: 'customer-service',
  brokers: kafkaBrokers
});
const producer: Producer = kafka.producer();
const consumer: Consumer = kafka.consumer({ groupId: preferenceSyncConsumerGroup });
let kafkaConnected = false;

function defaultCustomer(customerId: string): Customer {
  return {
    id: customerId,
    email: `${customerId}@example.com`,
    tier: 'STANDARD',
    preferences: {
      newsletter: true,
      language: 'en-US'
    }
  };
}

async function ensureProducerConnected(): Promise<void> {
  if (kafkaConnected) {
    return;
  }

  await producer.connect();
  kafkaConnected = true;
}

async function publishCustomerProfileUpdated(customer: Customer): Promise<void> {
  const event: CustomerProfileUpdatedEvent = {
    eventId: randomUUID(),
    customerId: customer.id,
    updatedAt: new Date().toISOString(),
    tier: customer.tier
  };

  await ensureProducerConnected();
  await producer.send({
    topic: profileUpdatedTopic,
    messages: [{ key: customer.id, value: JSON.stringify(event) }]
  });
}

function parsePreferenceSyncRequest(value: string): CustomerPreferenceSyncRequestEvent | null {
  try {
    const parsed = JSON.parse(value) as Partial<CustomerPreferenceSyncRequestEvent>;
    if (
      typeof parsed.requestId !== 'string' ||
      typeof parsed.customerId !== 'string' ||
      typeof parsed.requestedAt !== 'string'
    ) {
      return null;
    }

    return {
      requestId: parsed.requestId,
      customerId: parsed.customerId,
      requestedAt: parsed.requestedAt
    };
  } catch {
    return null;
  }
}

function buildPreferenceSyncReply(
  request: CustomerPreferenceSyncRequestEvent
): CustomerPreferenceSyncReplyEvent {
  const found = customerStore.get(request.customerId);
  const status: CustomerPreferenceSyncReplyEvent['status'] = found ? 'SUCCESS' : 'NOT_FOUND';

  return {
    requestId: request.requestId,
    customerId: request.customerId,
    status,
    syncedAt: new Date().toISOString(),
    preferenceVersion: 1,
    preferences: found
      ? {
          newsletter: String(found.preferences.newsletter),
          language: found.preferences.language
        }
      : undefined
  };
}

async function publishPreferenceSyncReply(reply: CustomerPreferenceSyncReplyEvent): Promise<void> {
  await ensureProducerConnected();
  await producer.send({
    topic: preferenceSyncReplyTopic,
    messages: [{ key: reply.customerId, value: JSON.stringify(reply) }]
  });
}

async function handlePreferenceSyncRequest(payload: EachMessagePayload): Promise<void> {
  const raw = payload.message.value?.toString();
  if (!raw) {
    return;
  }

  const request = parsePreferenceSyncRequest(raw);
  if (!request) {
    return;
  }

  const customer = customerStore.get(request.customerId) || defaultCustomer(request.customerId);
  await publishCustomerProfileUpdated(customer);
  await publishPreferenceSyncReply(buildPreferenceSyncReply(request));
}

async function startPreferenceSyncConsumer(): Promise<void> {
  await consumer.connect();
  await consumer.subscribe({ topic: preferenceSyncRequestTopic, fromBeginning: false });
  await consumer.run({ eachMessage: handlePreferenceSyncRequest });
}

async function publishAnalyticsNotification(event: AnalyticsNotificationEvent): Promise<void> {
  await ensureProducerConnected();
  await producer.send({
    topic: analyticsNotificationTopic,
    messages: [{ key: event.requestId, value: JSON.stringify(event) }]
  });
}

app.get('/customers/:customerId', (req: Request, res: Response) => {
  const { customerId } = req.params;
  if (customerId === 'missing') {
    res.sendStatus(404);
    return;
  }

  const customer = customerStore.get(customerId) || defaultCustomer(customerId);
  res.status(200).json(customer);
});

app.post('/customers', async (req: Request, res: Response) => {
  const payload = req.body || {};

  if (
    typeof payload.email !== 'string' ||
    !payload.email.includes('@') ||
    (payload.tier !== 'STANDARD' && payload.tier !== 'GOLD' && payload.tier !== 'PLATINUM') ||
    typeof payload.preferences?.newsletter !== 'boolean' ||
    typeof payload.preferences?.language !== 'string'
  ) {
    res.status(400).json({ error: 'Invalid customer payload' });
    return;
  }

  const customer: Customer = {
    id: randomUUID(),
    email: payload.email,
    tier: payload.tier,
    preferences: {
      newsletter: payload.preferences.newsletter,
      language: payload.preferences.language
    }
  };

  customerStore.set(customer.id, customer);
  try {
    await publishAnalyticsNotification({
      notificationId: randomUUID(),
      requestId: customer.id,
      title: 'CustomerCreated',
      body: `Customer ${customer.id} created`,
      priority: 'NORMAL'
    });
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`Failed to publish analytics notification on ${analyticsNotificationTopic}: ${message}`);
  }
  res.status(201).json(customer);
});

app.get('/customers/:customerId/preferences', (req: Request, res: Response) => {
  const { customerId } = req.params;
  if (customerId === 'missing') {
    res.sendStatus(404);
    return;
  }

  const customer = customerStore.get(customerId) || defaultCustomer(customerId);
  res.status(200).json(customer.preferences);
});

app.patch('/customers/:customerId/preferences', async (req: Request, res: Response) => {
  const { customerId } = req.params;
  const payload = req.body || {};

  if (typeof payload.newsletter !== 'boolean' || typeof payload.language !== 'string') {
    res.status(400).json({ error: 'Invalid preferences payload' });
    return;
  }

  const existing = customerStore.get(customerId) || defaultCustomer(customerId);
  const updated = {
    ...existing,
    preferences: {
      newsletter: payload.newsletter,
      language: payload.language
    }
  };

  customerStore.set(customerId, updated);

  try {
    await publishCustomerProfileUpdated(updated);
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`Failed to publish ${profileUpdatedTopic} event for customer ${customerId}: ${message}`);
  }

  try {
    await publishAnalyticsNotification({
      notificationId: randomUUID(),
      requestId: customerId,
      title: 'CustomerPreferencesUpdated',
      body: `Preferences updated for customer ${customerId}`,
      priority: 'NORMAL'
    });
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`Failed to publish analytics notification on ${analyticsNotificationTopic}: ${message}`);
  }

  res.status(200).json(updated.preferences);
});

async function start(): Promise<void> {
  await ensureProducerConnected();
  await startPreferenceSyncConsumer();
  app.listen(port, host, () => {
    console.log(`customer-service listening on http://${host}:${port}`);
  });
}

start().catch((error: unknown) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error(`Failed to start customer-service: ${message}`);
  process.exit(1);
});
