import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { Pool } from 'pg';

export type CallDirection = 'inbound' | 'outbound';
export type CallStatus =
  | 'ringing'
  | 'answered'
  | 'missed'
  | 'rejected'
  | 'disconnected'
  | 'failed';

export interface CallLog {
  callId: string;
  consultant: string;
  phoneNumber: string;
  direction: CallDirection;
  status: CallStatus;
  startTime: string;
  endTime?: string;
  durationSeconds?: number;
}

export interface AsteriskEvent {
  eventType: 'inbound' | 'oncall' | 'disconnected' | 'DNDon' | 'DNDoff';
  callId?: string;
  consultant?: string;
  phoneNumber?: string;
  direction?: CallDirection;
  status?: CallStatus;
  timestamp?: string;
  startTime?: string;
  endTime?: string;
  durationSeconds?: number;
}

@Injectable()
export class AppService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(AppService.name);
  private readonly calls = new Map<string, CallLog>();
  private readonly dndState = new Map<string, boolean>();
  private readonly pool?: Pool;

  constructor() {
    const dbType = process.env.DB_TYPE?.toLowerCase();
    const dbHost = process.env.DB_HOST;
    const dbPort = process.env.DB_PORT;
    const dbUser = process.env.DB_USER;
    const dbPass = process.env.DB_PASS;
    const dbName = process.env.DB_NAME;

    const canUseDiscreteConfig =
      dbType === 'postgres' && dbHost && dbPort && dbUser && dbPass && dbName;

    if (canUseDiscreteConfig) {
      this.pool = new Pool({
        host: dbHost,
        port: Number.parseInt(dbPort, 10),
        user: dbUser,
        password: dbPass,
        database: dbName,
      });
    }
  }

  async onModuleInit(): Promise<void> {
    if (!this.pool) {
      this.logger.warn(
        'Postgres envs (DB_TYPE/DB_HOST/DB_PORT/DB_USER/DB_PASS/DB_NAME) not set; using in-memory call log storage only.',
      );
      return;
    }
    await this.pool.query(`
      CREATE TABLE IF NOT EXISTS call_logs (
        call_id TEXT PRIMARY KEY,
        consultant TEXT NOT NULL,
        phone_number TEXT NOT NULL,
        direction TEXT NOT NULL,
        status TEXT NOT NULL,
        start_time TIMESTAMPTZ NOT NULL,
        end_time TIMESTAMPTZ NULL,
        duration_seconds INTEGER NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
    `);
    await this.pool.query(`
      CREATE TABLE IF NOT EXISTS consultant_dnd (
        consultant TEXT PRIMARY KEY,
        enabled BOOLEAN NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
    `);
  }

  async onModuleDestroy(): Promise<void> {
    if (this.pool) {
      await this.pool.end();
    }
  }

  getHealth() {
    return {
      status: 'ok',
      storage: this.pool ? 'postgres+memory' : 'memory',
      callsTracked: this.calls.size,
    };
  }

  async ingestAsteriskEvent(event: AsteriskEvent): Promise<void> {
    const timestamp = event.timestamp ?? new Date().toISOString();

    if (event.eventType === 'DNDon' || event.eventType === 'DNDoff') {
      if (!event.consultant) {
        throw new Error('consultant is required for DND events');
      }
      const enabled = event.eventType === 'DNDon';
      await this.setDnd(event.consultant, enabled);
      return;
    }

    if (!event.callId || !event.consultant || !event.phoneNumber) {
      throw new Error('callId, consultant and phoneNumber are required for call events');
    }

    const existing = this.calls.get(event.callId);
    const direction = event.direction ?? (event.eventType === 'inbound' ? 'inbound' : 'outbound');

    const baseCall: CallLog =
      existing ?? {
        callId: event.callId,
        consultant: event.consultant,
        phoneNumber: event.phoneNumber,
        direction,
        status: 'ringing',
        startTime: event.startTime ?? timestamp,
      };

    if (event.eventType === 'inbound' || event.eventType === 'oncall') {
      baseCall.status = event.status ?? 'ringing';
      baseCall.direction = direction;
      baseCall.startTime = event.startTime ?? baseCall.startTime;
    }

    if (event.eventType === 'disconnected') {
      baseCall.status = event.status ?? 'disconnected';
      baseCall.endTime = event.endTime ?? timestamp;
      if (event.durationSeconds !== undefined) {
        baseCall.durationSeconds = event.durationSeconds;
      } else if (baseCall.startTime) {
        const seconds =
          (Date.parse(baseCall.endTime) - Date.parse(baseCall.startTime)) / 1000;
        baseCall.durationSeconds = Math.max(0, Math.floor(seconds));
      }
    }

    this.calls.set(baseCall.callId, baseCall);
    await this.persistCall(baseCall);
  }

  async setDnd(consultant: string, enabled: boolean): Promise<void> {
    this.dndState.set(consultant, enabled);
    if (!this.pool) {
      return;
    }
    await this.pool.query(
      `INSERT INTO consultant_dnd (consultant, enabled, updated_at)
       VALUES ($1, $2, NOW())
       ON CONFLICT (consultant)
       DO UPDATE SET enabled = EXCLUDED.enabled, updated_at = NOW()`,
      [consultant, enabled],
    );
  }

  getDndSnapshot(): Record<string, boolean> {
    return Object.fromEntries(this.dndState.entries());
  }

  listCalls(limit = 100, consultant?: string): CallLog[] {
    const allCalls = [...this.calls.values()];
    const filtered = consultant
      ? allCalls.filter((call) => call.consultant === consultant)
      : allCalls;
    return filtered
      .sort((a, b) => Date.parse(b.startTime) - Date.parse(a.startTime))
      .slice(0, Math.max(1, Math.min(limit, 500)));
  }

  getCall(callId: string): CallLog | undefined {
    return this.calls.get(callId);
  }

  private async persistCall(call: CallLog): Promise<void> {
    if (!this.pool) {
      return;
    }
    await this.pool.query(
      `INSERT INTO call_logs (
          call_id, consultant, phone_number, direction, status, start_time, end_time, duration_seconds, updated_at
       )
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,NOW())
       ON CONFLICT (call_id)
       DO UPDATE SET
          consultant = EXCLUDED.consultant,
          phone_number = EXCLUDED.phone_number,
          direction = EXCLUDED.direction,
          status = EXCLUDED.status,
          start_time = EXCLUDED.start_time,
          end_time = EXCLUDED.end_time,
          duration_seconds = EXCLUDED.duration_seconds,
          updated_at = NOW()`,
      [
        call.callId,
        call.consultant,
        call.phoneNumber,
        call.direction,
        call.status,
        call.startTime,
        call.endTime ?? null,
        call.durationSeconds ?? null,
      ],
    );
  }
}
