export type CustomerPreferences = {
  newsletter: boolean;
  language: string;
};

export type Customer = {
  id: string;
  email: string;
  tier: 'STANDARD' | 'GOLD' | 'PLATINUM';
  preferences: CustomerPreferences;
};

export type CustomerProfileUpdatedEvent = {
  eventId: string;
  customerId: string;
  updatedAt: string;
  tier: Customer['tier'];
};

export type AnalyticsNotificationEvent = {
  notificationId: string;
  requestId: string;
  title: string;
  body: string;
  priority: 'LOW' | 'NORMAL' | 'HIGH';
};

export type CustomerPreferenceSyncRequestEvent = {
  requestId: string;
  customerId: string;
  requestedAt: string;
};

export type CustomerPreferenceSyncReplyEvent = {
  requestId: string;
  customerId: string;
  status: 'SUCCESS' | 'NOT_FOUND';
  syncedAt: string;
  preferenceVersion: number;
  preferences?: Record<string, string>;
};
