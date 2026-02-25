import express, { type Request, type Response } from 'express';
import { randomUUID } from 'node:crypto';
import { Kafka, type Producer } from 'kafkajs';
import mqtt from 'mqtt';
import type {
  AnalyticsNotificationEvent,
  Customer,
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
const analyticsMqttUrl = process.env.ANALYTICS_MQTT_URL || 'mqtt://localhost:1883';
const analyticsNotificationTopic = process.env.ANALYTICS_NOTIFICATION_TOPIC || 'notification/user';

const customerStore = new Map<string, Customer>();
const kafka = new Kafka({
  clientId: 'customer-service',
  brokers: kafkaBrokers
});
const producer: Producer = kafka.producer();
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

function publishAnalyticsNotification(event: AnalyticsNotificationEvent): void {
  const client = mqtt.connect(analyticsMqttUrl, { reconnectPeriod: 0, connectTimeout: 1000 });
  const payload = JSON.stringify(event);
  let completed = false;

  const done = (): void => {
    if (completed) {
      return;
    }

    completed = true;
    client.end(true);
  };

  const timeout = setTimeout(() => {
    done();
  }, 1500);

  client.once('connect', () => {
    client.publish(analyticsNotificationTopic, payload, { qos: 1 }, (error?: Error | null) => {
      if (error) {
        console.error(`Failed to publish analytics notification on ${analyticsNotificationTopic}: ${error.message}`);
      }

      clearTimeout(timeout);
      done();
    });
  });

  client.once('error', (error: Error) => {
    console.error(`Failed to connect to analytics MQTT broker (${analyticsMqttUrl}): ${error.message}`);
    clearTimeout(timeout);
    done();
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

app.post('/customers', (req: Request, res: Response) => {
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
  publishAnalyticsNotification({
    notificationId: randomUUID(),
    requestId: customer.id,
    title: 'CustomerCreated',
    body: `Customer ${customer.id} created`,
    priority: 'NORMAL'
  });
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

  publishAnalyticsNotification({
    notificationId: randomUUID(),
    requestId: customerId,
    title: 'CustomerPreferencesUpdated',
    body: `Preferences updated for customer ${customerId}`,
    priority: 'NORMAL'
  });

  res.status(200).json(updated.preferences);
});

app.listen(port, host, () => {
  console.log(`customer-service listening on http://${host}:${port}`);
});
