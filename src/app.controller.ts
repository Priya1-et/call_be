import {
  BadRequestException,
  Body,
  Controller,
  Get,
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
  constructor(private readonly appService: AppService) {}

  @Get()
  getHealth() {
    return this.appService.getHealth();
  }

  @Post('v1/asterisk/events')
  async ingestAsteriskEvent(@Body() body: AsteriskEvent) {
    try {
      await this.appService.ingestAsteriskEvent(body);
      return { accepted: true };
    } catch (error) {
      throw new BadRequestException(
        error instanceof Error ? error.message : 'Invalid event payload',
      );
    }
  }

  @Get('v1/calls')
  getCalls(@Query('limit') limit?: string, @Query('consultant') consultant?: string) {
    const parsedLimit = limit ? Number.parseInt(limit, 10) : 100;
    return this.appService.listCalls(Number.isNaN(parsedLimit) ? 100 : parsedLimit, consultant);
  }

  @Get('v1/calls/:callId')
  getCall(@Param('callId') callId: string) {
    const call = this.appService.getCall(callId);
    if (!call) {
      throw new NotFoundException(`No call found for callId=${callId}`);
    }
    return call;
  }

  @Get('v1/dnd')
  getDndSnapshot() {
    return this.appService.getDndSnapshot();
  }

  @Put('v1/dnd/:consultant')
  async setDndState(
    @Param('consultant') consultant: string,
    @Body() body: { enabled?: boolean },
  ) {
    if (typeof body.enabled !== 'boolean') {
      throw new BadRequestException('Body requires boolean field: enabled');
    }
    await this.appService.setDnd(consultant, body.enabled);
    return { consultant, enabled: body.enabled };
  }
}
