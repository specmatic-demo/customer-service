import express, { type Request, type Response } from 'express';

const app = express();
app.use(express.json({ limit: '1mb' }));

const host = process.env.CUSTOMER_HOST || '0.0.0.0';
const port = Number.parseInt(process.env.CUSTOMER_PORT || '9000', 10);

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

const customerStore = new Map<string, Customer>();

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

app.get('/customers/:customerId', (req: Request, res: Response) => {
  const { customerId } = req.params;
  if (customerId === 'missing') {
    res.sendStatus(404);
    return;
  }

  const customer = customerStore.get(customerId) || defaultCustomer(customerId);
  res.status(200).json(customer);
});

app.patch('/customers/:customerId/preferences', (req: Request, res: Response) => {
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
  res.status(200).json(updated.preferences);
});

app.listen(port, host, () => {
  console.log(`customer-service listening on http://${host}:${port}`);
});
