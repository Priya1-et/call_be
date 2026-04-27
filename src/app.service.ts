import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { Pool } from 'pg';
import * as fs from 'fs';
import * as path from 'path';

export type CallDirection = 'inbound' | 'outbound';
export type CallStatus =
  | 'ringing'
  | 'answered'
  | 'missed'
  | 'rejected'
  | 'disconnected'
  | 'failed';

export type CallDisposition =
  | 'ANSWERED'
  | 'NO_ANSWER'
  | 'FAILED'
  | 'REJECTED'
  | 'BUSY'
  | 'CANCELLED'
  | 'UNKNOWN';

export type CallEndReason =
  | 'local_hangup'
  | 'remote_hangup'
  | 'cancelled'
  | 'rejected'
  | 'failed'
  | 'no_answer'
  | 'unknown';

export interface CallLog {
  callId: string;
  consultant: string;
  phoneNumber: string;
  direction: CallDirection;
  status: CallStatus;
  startTime: string;
  endTime?: string;
  durationSeconds?: number;
  // Enriched disposition tracking
  answeredAt?: string;
  ringSeconds?: number;
  billsec?: number;
  disposition?: CallDisposition;
  endReason?: CallEndReason;
  // SIP response from Iagu / remote when call fails
  sipResponseCode?: number;
  sipResponseReason?: string;
  // Outgoing caller-id used (for outbound)
  outgoingNumber?: string;
  // Path to the recording file written by Asterisk MixMonitor (relative)
  recordingFile?: string;
}

export interface AsteriskEvent {
  eventType:
    | 'inbound'
    | 'oncall'
    | 'disconnected'
    | 'failed'
    | 'DNDon'
    | 'DNDoff';
  callId?: string;
  consultant?: string;
  phoneNumber?: string;
  direction?: CallDirection;
  status?: CallStatus;
  timestamp?: string;
  startTime?: string;
  endTime?: string;
  durationSeconds?: number;
  outgoingNumber?: string;
  // Failure / disposition details from frontend
  sipResponseCode?: number;
  sipResponseReason?: string;
  endReason?: CallEndReason;
  recordingFile?: string;
}

@Injectable()
export class AppService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(AppService.name);
  private readonly calls = new Map<string, CallLog>();
  private readonly dndState = new Map<string, boolean>();
  private readonly pool?: Pool;

  // File logging
  private readonly logDir: string;
  private readonly eventLogPath: string;
  private readonly summaryLogPath: string;

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

    this.logDir =
      process.env.CALL_LOG_DIR ?? path.resolve(process.cwd(), 'logs');
    this.eventLogPath = path.join(this.logDir, 'call-events.jsonl');
    this.summaryLogPath = path.join(this.logDir, 'call-summary.jsonl');
    this.ensureLogDir();
  }

  private ensureLogDir(): void {
    try {
      if (!fs.existsSync(this.logDir)) {
        fs.mkdirSync(this.logDir, { recursive: true });
      }
    } catch (err) {
      this.logger.error(
        `[file-log] failed to create log dir ${this.logDir}: ${
          err instanceof Error ? err.message : String(err)
        }`,
      );
    }
  }

  private appendJsonl(filePath: string, payload: unknown): void {
    try {
      fs.appendFileSync(filePath, JSON.stringify(payload) + '\n', 'utf8');
    } catch (err) {
      this.logger.error(
        `[file-log] failed to append to ${filePath}: ${
          err instanceof Error ? err.message : String(err)
        }`,
      );
    }
  }

  async onModuleInit(): Promise<void> {
    this.logger.log(
      `[file-log] events=${this.eventLogPath} summary=${this.summaryLogPath}`,
    );
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
        answered_at TIMESTAMPTZ NULL,
        ring_seconds INTEGER NULL,
        billsec INTEGER NULL,
        disposition TEXT NULL,
        end_reason TEXT NULL,
        sip_response_code INTEGER NULL,
        sip_response_reason TEXT NULL,
        outgoing_number TEXT NULL,
        recording_file TEXT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
    `);
    // Backfill columns if table already existed without them
    await this.pool.query(`
      ALTER TABLE call_logs
        ADD COLUMN IF NOT EXISTS answered_at TIMESTAMPTZ NULL,
        ADD COLUMN IF NOT EXISTS ring_seconds INTEGER NULL,
        ADD COLUMN IF NOT EXISTS billsec INTEGER NULL,
        ADD COLUMN IF NOT EXISTS disposition TEXT NULL,
        ADD COLUMN IF NOT EXISTS end_reason TEXT NULL,
        ADD COLUMN IF NOT EXISTS sip_response_code INTEGER NULL,
        ADD COLUMN IF NOT EXISTS sip_response_reason TEXT NULL,
        ADD COLUMN IF NOT EXISTS outgoing_number TEXT NULL,
        ADD COLUMN IF NOT EXISTS recording_file TEXT NULL;
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
      eventLog: this.eventLogPath,
      summaryLog: this.summaryLogPath,
    };
  }

  async ingestAsteriskEvent(event: AsteriskEvent): Promise<void> {
    const timestamp = event.timestamp ?? new Date().toISOString();

    // Always write the raw event to the JSONL events file (audit trail)
    this.appendJsonl(this.eventLogPath, {
      receivedAt: timestamp,
      ...event,
    });

    this.logger.log(
      `[event.ingest] type=${event.eventType} callId=${event.callId ?? '-'} consultant=${event.consultant ?? '-'} phone=${event.phoneNumber ?? '-'}`,
    );

    if (event.eventType === 'DNDon' || event.eventType === 'DNDoff') {
      if (!event.consultant) {
        this.logger.warn('[event.ingest] DND event missing consultant');
        throw new Error('consultant is required for DND events');
      }
      const enabled = event.eventType === 'DNDon';
      await this.setDnd(event.consultant, enabled);
      this.logger.log(
        `[event.ingest] DND consultant=${event.consultant} enabled=${enabled}`,
      );
      return;
    }

    if (!event.callId || !event.consultant || !event.phoneNumber) {
      this.logger.warn(
        '[event.ingest] missing required fields for call event',
      );
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

    if (event.outgoingNumber) {
      baseCall.outgoingNumber = event.outgoingNumber;
    }

    // RINGING state
    if (event.eventType === 'inbound' || (event.eventType === 'oncall' && event.status === 'ringing')) {
      baseCall.status = event.status ?? 'ringing';
      baseCall.direction = direction;
      baseCall.startTime = event.startTime ?? baseCall.startTime;
    }

    // ANSWERED state — capture answeredAt
    if (event.eventType === 'oncall' && event.status === 'answered') {
      baseCall.status = 'answered';
      baseCall.direction = direction;
      if (!baseCall.answeredAt) {
        baseCall.answeredAt = event.timestamp ?? timestamp;
      }
    }

    // FAILED — call never connected, came back with SIP error
    if (event.eventType === 'failed') {
      baseCall.status = 'failed';
      baseCall.endTime = event.endTime ?? timestamp;
      baseCall.sipResponseCode = event.sipResponseCode;
      baseCall.sipResponseReason = event.sipResponseReason;
      baseCall.endReason = event.endReason ?? 'failed';
      baseCall.disposition = this.dispositionFromSip(event.sipResponseCode);
      this.computeDurations(baseCall);
    }

    // DISCONNECTED — call ended, decide disposition
    if (event.eventType === 'disconnected') {
      baseCall.status = event.status ?? 'disconnected';
      baseCall.endTime = event.endTime ?? timestamp;
      if (event.recordingFile) baseCall.recordingFile = event.recordingFile;
      if (event.sipResponseCode) baseCall.sipResponseCode = event.sipResponseCode;
      if (event.sipResponseReason) baseCall.sipResponseReason = event.sipResponseReason;
      if (event.endReason) baseCall.endReason = event.endReason;
      this.computeDurations(baseCall);

      // Decide disposition if not already set
      if (!baseCall.disposition) {
        if (baseCall.answeredAt) {
          baseCall.disposition = 'ANSWERED';
        } else if (baseCall.sipResponseCode) {
          baseCall.disposition = this.dispositionFromSip(baseCall.sipResponseCode);
        } else if (baseCall.endReason === 'cancelled') {
          baseCall.disposition = 'CANCELLED';
        } else {
          baseCall.disposition = 'NO_ANSWER';
        }
      }

      // Default endReason if missing
      if (!baseCall.endReason) {
        baseCall.endReason = baseCall.answeredAt ? 'remote_hangup' : 'no_answer';
      }

      // Write summary line for ended calls
      this.appendJsonl(this.summaryLogPath, {
        finalisedAt: timestamp,
        ...baseCall,
      });
    }

    this.calls.set(baseCall.callId, baseCall);
    await this.persistCall(baseCall);
    this.logger.log(
      `[event.ingest] saved callId=${baseCall.callId} status=${baseCall.status} disposition=${baseCall.disposition ?? '-'} billsec=${baseCall.billsec ?? '-'}`,
    );
  }

  private computeDurations(call: CallLog): void {
    if (!call.endTime) return;
    const end = Date.parse(call.endTime);
    const start = Date.parse(call.startTime);
    if (!Number.isNaN(end) && !Number.isNaN(start)) {
      call.durationSeconds = Math.max(0, Math.floor((end - start) / 1000));
    }
    if (call.answeredAt) {
      const answered = Date.parse(call.answeredAt);
      if (!Number.isNaN(end) && !Number.isNaN(answered)) {
        call.billsec = Math.max(0, Math.floor((end - answered) / 1000));
      }
      if (!Number.isNaN(answered) && !Number.isNaN(start)) {
        call.ringSeconds = Math.max(0, Math.floor((answered - start) / 1000));
      }
    } else {
      call.billsec = 0;
      call.ringSeconds = call.durationSeconds;
    }
  }

  private dispositionFromSip(code?: number): CallDisposition {
    if (!code) return 'UNKNOWN';
    if (code === 486 || code === 600) return 'BUSY';
    if (code === 487) return 'CANCELLED';
    if (code === 603) return 'REJECTED';
    if (code === 480 || code === 408) return 'NO_ANSWER';
    if (code >= 400) return 'FAILED';
    return 'UNKNOWN';
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

  isDndEnabled(consultant: string): boolean {
    return this.dndState.get(consultant) ?? false;
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

  /**
   * Read the last N lines of the JSONL event log (for /v1/logs/events endpoint).
   */
  readEventLogTail(limit = 200): unknown[] {
    return this.readJsonlTail(this.eventLogPath, limit);
  }

  /**
   * Read the last N lines of the JSONL summary log (for /v1/logs/summary endpoint).
   */
  readSummaryLogTail(limit = 200): unknown[] {
    return this.readJsonlTail(this.summaryLogPath, limit);
  }

  /**
   * Read all events for a specific callId from the events JSONL.
   */
  readCallEvents(callId: string, limit = 500): unknown[] {
    if (!fs.existsSync(this.eventLogPath)) return [];
    try {
      const content = fs.readFileSync(this.eventLogPath, 'utf8');
      const lines = content.split('\n').filter((l) => l.trim().length > 0);
      const matches: unknown[] = [];
      for (const line of lines) {
        try {
          const obj = JSON.parse(line) as { callId?: string };
          if (obj.callId === callId) matches.push(obj);
        } catch {
          // ignore bad lines
        }
      }
      return matches.slice(-Math.max(1, Math.min(limit, 1000)));
    } catch {
      return [];
    }
  }

  private readJsonlTail(filePath: string, limit: number): unknown[] {
    if (!fs.existsSync(filePath)) return [];
    try {
      const content = fs.readFileSync(filePath, 'utf8');
      const lines = content
        .split('\n')
        .filter((l) => l.trim().length > 0)
        .slice(-Math.max(1, Math.min(limit, 1000)));
      return lines
        .map((l) => {
          try {
            return JSON.parse(l);
          } catch {
            return null;
          }
        })
        .filter((v) => v !== null);
    } catch {
      return [];
    }
  }

  private async persistCall(call: CallLog): Promise<void> {
    if (!this.pool) {
      return;
    }
    await this.pool.query(
      `INSERT INTO call_logs (
          call_id, consultant, phone_number, direction, status,
          start_time, end_time, duration_seconds,
          answered_at, ring_seconds, billsec, disposition, end_reason,
          sip_response_code, sip_response_reason, outgoing_number, recording_file,
          updated_at
       )
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,NOW())
       ON CONFLICT (call_id)
       DO UPDATE SET
          consultant = EXCLUDED.consultant,
          phone_number = EXCLUDED.phone_number,
          direction = EXCLUDED.direction,
          status = EXCLUDED.status,
          start_time = EXCLUDED.start_time,
          end_time = EXCLUDED.end_time,
          duration_seconds = EXCLUDED.duration_seconds,
          answered_at = EXCLUDED.answered_at,
          ring_seconds = EXCLUDED.ring_seconds,
          billsec = EXCLUDED.billsec,
          disposition = EXCLUDED.disposition,
          end_reason = EXCLUDED.end_reason,
          sip_response_code = EXCLUDED.sip_response_code,
          sip_response_reason = EXCLUDED.sip_response_reason,
          outgoing_number = EXCLUDED.outgoing_number,
          recording_file = EXCLUDED.recording_file,
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
        call.answeredAt ?? null,
        call.ringSeconds ?? null,
        call.billsec ?? null,
        call.disposition ?? null,
        call.endReason ?? null,
        call.sipResponseCode ?? null,
        call.sipResponseReason ?? null,
        call.outgoingNumber ?? null,
        call.recordingFile ?? null,
      ],
    );
  }
}
