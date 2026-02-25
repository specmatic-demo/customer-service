import express, { type Request, type Response } from 'express';
import { randomUUID } from 'node:crypto';
import { Kafka, type Producer } from 'kafkajs';

const app = express();
app.use(express.json({ limit: '1mb' }));

const host = process.env.CUSTOMER_HOST || '0.0.0.0';
const port = Number.parseInt(process.env.CUSTOMER_PORT || '9000', 10);
const kafkaBrokers = (process.env.CUSTOMER_KAFKA_BROKERS || 'localhost:5411')
  .split(',')
  .map((value) => value.trim())
  .filter(Boolean);
const profileUpdatedTopic = process.env.CUSTOMER_PROFILE_UPDATED_TOPIC || 'customer.profile.updated';

type CustomerPreferences = {
  newsletter: boolean;
  language: string;
};

type Customer = {
  id: string;
  email: string;
  tier: 'STANDARD' | 'GOLD' | 'PLATINUM';
  preferences: CustomerPreferences;
};

type CustomerProfileUpdatedEvent = {
  eventId: string;
  customerId: string;
  updatedAt: string;
  tier: Customer['tier'];
};

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

app.get('/customers/:customerId', (req: Request, res: Response) => {
  const { customerId } = req.params;
  if (customerId === 'missing') {
    res.sendStatus(404);
    return;
  }

  const customer = customerStore.get(customerId) || defaultCustomer(customerId);
  res.status(200).json(customer);
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

  res.status(200).json(updated.preferences);
});

app.listen(port, host, () => {
  console.log(`customer-service listening on http://${host}:${port}`);
});
