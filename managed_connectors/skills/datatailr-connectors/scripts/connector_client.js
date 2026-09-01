/* Vendor into a shared Datatailr app. Contains no credentials. */
export class ConnectorGatewayError extends Error {}

export class ConnectorClient {
  constructor(baseUrl = null) {
    const parts = window.location.pathname.split('/');
    const environment = parts[1] === 'job' && parts[2] ? parts[2] : 'dev';
    this.baseUrl = (baseUrl || `${window.location.origin}/job/${environment}/connector-gateway`).replace(/\/$/, '');
  }

  async request(method, path, payload = null) {
    const response = await fetch(this.baseUrl + path, {
      method,
      credentials: 'same-origin',
      headers: {
        'Content-Type': 'application/json',
        'X-Datatailr-Connector-Client': '1',
      },
      body: payload === null ? undefined : JSON.stringify(payload),
    });
    const data = await response.json().catch(() => ({}));
    if (!response.ok) {
      throw new ConnectorGatewayError(data.error || `Connector gateway returned HTTP ${response.status}`);
    }
    return data;
  }

  async connections() {
    return (await this.request('GET', '/v1/connections')).connections;
  }

  async query(capability, parameters = {}) {
    return (await this.request('POST', '/v1/query', { capability, parameters })).data;
  }

  async upcomingOutlookEvents({ days = 14, timeZone = 'UTC', limit = 20 } = {}) {
    return this.query('outlook.calendar.events.upcoming', {
      days,
      time_zone: timeZone,
      limit,
    });
  }

  async outlookEvents(start, end, { timeZone = 'UTC', limit = 20 } = {}) {
    return this.query('outlook.calendar.events.range', {
      start,
      end,
      time_zone: timeZone,
      limit,
    });
  }

  async outlookAvailability(schedules, start, end, { timeZone = 'UTC', intervalMinutes = 30 } = {}) {
    return this.query('outlook.calendar.availability', {
      schedules,
      start,
      end,
      time_zone: timeZone,
      interval_minutes: intervalMinutes,
    });
  }

  async action(capability, parameters = {}) {
    return this.request('POST', '/v1/actions', { capability, parameters });
  }
}
