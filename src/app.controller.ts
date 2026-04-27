import {
  BadRequestException,
  Body,
  Controller,
  Get,
  Logger,
  NotFoundException,
  Param,
  Post,
  Put,
  Query,
} from '@nestjs/common';
import { AppService } from './app.service';
import type { AsteriskEvent } from './app.service';

@Controller()
export class AppController {
  private readonly logger = new Logger(AppController.name);

  constructor(private readonly appService: AppService) {}

  @Get()
  getHealth() {
    this.logger.log('[GET /] health check');
    return this.appService.getHealth();
  }

  @Post('v1/asterisk/events')
  async ingestAsteriskEvent(@Body() body: AsteriskEvent) {
    this.logger.log(`[POST /v1/asterisk/events] payload=${JSON.stringify(body)}`);
    try {
      await this.appService.ingestAsteriskEvent(body);
      this.logger.log(
        `[POST /v1/asterisk/events] accepted callId=${body.callId ?? '-'} type=${body.eventType}`,
      );
      return { accepted: true };
    } catch (error) {
      const msg = error instanceof Error ? error.message : 'Invalid event payload';
      this.logger.error(`[POST /v1/asterisk/events] rejected: ${msg}`);
      throw new BadRequestException(msg);
    }
  }

  @Get('v1/calls')
  getCalls(@Query('limit') limit?: string, @Query('consultant') consultant?: string) {
    this.logger.log(`[GET /v1/calls] limit=${limit ?? '-'} consultant=${consultant ?? '-'}`);
    const parsedLimit = limit ? Number.parseInt(limit, 10) : 100;
    const result = this.appService.listCalls(
      Number.isNaN(parsedLimit) ? 100 : parsedLimit,
      consultant,
    );
    this.logger.log(`[GET /v1/calls] returning ${result.length} call(s)`);
    return result;
  }

  @Get('v1/calls/:callId')
  getCall(@Param('callId') callId: string) {
    this.logger.log(`[GET /v1/calls/${callId}]`);
    const call = this.appService.getCall(callId);
    if (!call) {
      this.logger.warn(`[GET /v1/calls/${callId}] not found`);
      throw new NotFoundException(`No call found for callId=${callId}`);
    }
    return call;
  }

  @Get('v1/dnd')
  getDndSnapshot() {
    this.logger.log('[GET /v1/dnd]');
    return this.appService.getDndSnapshot();
  }

  @Get('v1/dnd/:consultant')
  getDndState(@Param('consultant') consultant: string) {
    const enabled = this.appService.isDndEnabled(consultant);
    this.logger.log(`[GET /v1/dnd/${consultant}] enabled=${enabled}`);
    return { consultant, enabled };
  }

  @Put('v1/dnd/:consultant')
  async setDndState(
    @Param('consultant') consultant: string,
    @Body() body: { enabled?: boolean },
  ) {
    this.logger.log(
      `[PUT /v1/dnd/${consultant}] payload=${JSON.stringify(body)}`,
    );
    if (typeof body.enabled !== 'boolean') {
      this.logger.warn(`[PUT /v1/dnd/${consultant}] missing enabled boolean`);
      throw new BadRequestException('Body requires boolean field: enabled');
    }
    await this.appService.setDnd(consultant, body.enabled);
    this.logger.log(`[PUT /v1/dnd/${consultant}] enabled=${body.enabled}`);
    return { consultant, enabled: body.enabled };
  }
}
