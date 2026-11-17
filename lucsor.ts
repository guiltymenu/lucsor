import { searcherClient, SearcherClient } from 'jito-ts/dist/sdk/block-engine/searcher';
import { Bundle } from 'jito-ts/dist/sdk/block-engine/types';
import type { BundleResult } from 'jito-ts/dist/gen/block-engine/bundle';
import { Keypair, PublicKey } from '@solana/web3.js';
import type { Logger } from 'pino';
import type {
  BundleGateway,
  BundleReceipt,
  BundleSubmission,
  LandingMetrics,
  StrategyLandingStats,
} from '../../domain/trading/BundleGateway';
import type { Clock } from '../../domain/shared/Clock';
import type { LoggerPort } from '../../domain/shared/LoggerPort';
import type { MetricsSink } from '../../domain/shared/MetricsSink';

export interface JitoGatewayConfig {
  readonly blockEngineUrl: string;
  readonly tipLamports: number;
  readonly maxAttempts: number;
  readonly baseDelayMs: number;
  readonly maxDelayMs: number;
  readonly resubscribeBaseDelayMs: number;
  readonly resubscribeMaxDelayMs: number;
}

export class PinoLoggerAdapter implements LoggerPort {
  constructor(private readonly delegate: Logger) {}

  info(message: string, context?: Record<string, unknown>): void {
    this.delegate.info(context ?? {}, message);
  }

  warn(message: string, context?: Record<string, unknown>): void {
    this.delegate.warn(context ?? {}, message);
  }

  error(message: string, context?: Record<string, unknown>): void {
    this.delegate.error(context ?? {}, message);
  }
}

export class BundleSubmissionError extends Error {
  constructor(
    message: string,
    readonly strategyId: string,
    readonly attempts: number,
  ) {
    super(message);
    this.name = 'BundleSubmissionError';
  }
}

export class TipAccountsUnavailable extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'TipAccountsUnavailable';
  }
}

interface PendingBundle {
  readonly strategyId: string;
  readonly submittedAtMs: number;
}

interface MutableCounters {
  submitted: number;
  landed: number;
  rejected: number;
}

export class JitoBundleGateway implements BundleGateway, LandingMetrics {
  private readonly client: SearcherClient;
  private readonly pending = new Map<string, PendingBundle>();
  private readonly counters = new Map<string, MutableCounters>();
  private cancelSubscription: (() => void) | null = null;
  private resubscribeAttempts = 0;
  private tipAccounts: PublicKey[] | null = null;
  private tipCursor = 0;
  private running = false;

  constructor(
    private readonly config: JitoGatewayConfig,
    private readonly signer: Keypair,
    private readonly logger: LoggerPort,
    private readonly clock: Clock,
    private readonly metrics: MetricsSink,
    client?: SearcherClient,
  ) {
    this.client = client ?? searcherClient(config.blockEngineUrl, signer);
  }

  start(): void {
    if (this.running) {
      return;
    }
    this.running = true;
    this.openResultStream();
  }

  stop(): void {
    this.running = false;
    this.closeResultStream();
  }

  async submit(submission: BundleSubmission): Promise<BundleReceipt> {
    if (submission.transactions.length > 4) {
      throw new BundleSubmissionError(
        `Bundle has ${submission.transactions.length} transactions, at most 4 are allowed.`,
        submission.strategyId,
        0,
      );
    }
    let lastError = 'Unknown failure.';

    for (let attempt = 1; attempt <= this.config.maxAttempts; attempt++) {
      try {
        const bundleId = await this.sendOnce(submission);
        this.pending.set(bundleId, {
          strategyId: submission.strategyId,
          submittedAtMs: this.clock.nowMs(),
        });
        this.countersFor(submission.strategyId).submitted += 1;
        this.metrics.increment('jitoBundleSubmitted', { strategy: submission.strategyId });
        this.logger.info('Bundle submitted.', {
          bundleId,
          strategyId: submission.strategyId,
          attempt,
        });
        return { bundleId, strategyId: submission.strategyId, attempts: attempt };
      } catch (error) {
        lastError = error instanceof Error ? error.message : String(error);
        this.metrics.increment('jitoBundleSubmitFailed', { strategy: submission.strategyId });
        this.logger.warn('Submission attempt failed. Retrying submission.', {
          strategyId: submission.strategyId,
          attempt,
          reason: lastError,
        });
        if (attempt < this.config.maxAttempts) {
          await this.clock.sleep(this.backoffDelay(attempt));
        }
      }
    }

    throw new BundleSubmissionError(lastError, submission.strategyId, this.config.maxAttempts);
  }

  landRate(strategyId: string): number {
    const counters = this.counters.get(strategyId);
    if (counters === undefined || counters.submitted === 0) {
      return 0;
    }
    return counters.landed / counters.submitted;
  }

  snapshot(): ReadonlyArray<StrategyLandingStats> {
    return Array.from(this.counters.entries()).map(([strategyId, counters]) => ({
      strategyId,
      submitted: counters.submitted,
      landed: counters.landed,
      rejected: counters.rejected,
      landRate: counters.submitted === 0 ? 0 : counters.landed / counters.submitted,
    }));
  }

  private async sendOnce(submission: BundleSubmission): Promise<string> {
    const tipAccount = await this.pickTipAccount();
    const base = new Bundle([...submission.transactions], 5);
    const tipped = base.addTipTx(this.signer, this.config.tipLamports, tipAccount, submission.blockhash);
    if (tipped instanceof Error) {
      throw tipped;
    }

    const response = await this.client.sendBundle(tipped);
    if (!response.ok) {
      throw new Error('Block engine rejected the bundle.');
    }
    return response.value;
  }

  private async pickTipAccount(): Promise<PublicKey> {
    if (this.tipAccounts === null) {
      const response = await this.client.getTipAccounts();
      if (!response.ok) {
        throw new TipAccountsUnavailable('Unable to retrieve tip accounts.');
      }
      if (response.value.length === 0) {
        throw new TipAccountsUnavailable('Block engine returned no tip accounts.');
      }
      this.tipAccounts = response.value.map((address) => new PublicKey(address));
    }
    const chosen = this.tipAccounts[this.tipCursor % this.tipAccounts.length];
    this.tipCursor += 1;
    return chosen;
  }

  private backoffDelay(attempt: number): number {
    return Math.min(this.config.baseDelayMs * Math.pow(2, attempt - 1), this.config.maxDelayMs);
  }

  private openResultStream(): void {
    this.cancelSubscription = this.client.onBundleResult(
      (result) => this.handleResult(result),
      (error) => this.handleStreamError(error),
    );
  }

  private closeResultStream(): void {
    if (this.cancelSubscription !== null) {
      this.cancelSubscription();
      this.cancelSubscription = null;
    }
  }

  private handleStreamError(error: Error): void {
    this.closeResultStream();
    if (!this.running) {
      return;
    }
    this.resubscribeAttempts += 1;
    const delay = Math.min(
      this.config.resubscribeBaseDelayMs * Math.pow(2, this.resubscribeAttempts - 1),
      this.config.resubscribeMaxDelayMs,
    );
    this.metrics.increment('jitoStreamReconnect', {});
    this.logger.error('Bundle result stream failed. Reconnecting.', {
      reason: error.message,
      attempt: this.resubscribeAttempts,
      delayMs: delay,
    });
    void this.clock.sleep(delay).then(() => {
      if (this.running && this.cancelSubscription === null) {
        this.openResultStream();
      }
    });
  }

  private handleResult(result: BundleResult): void {
    this.resubscribeAttempts = 0;
    const entry = this.pending.get(result.bundleId);
    if (entry === undefined) {
      return;
    }

    if (result.processed !== undefined || result.finalized !== undefined) {
      this.pending.delete(result.bundleId);
      this.countersFor(entry.strategyId).landed += 1;
      this.metrics.increment('jitoBundleLanded', { strategy: entry.strategyId });
      this.publishRate(entry.strategyId);
      this.metrics.timing('jitoBundleLandLatencyMs', this.clock.nowMs() - entry.submittedAtMs, {
        strategy: entry.strategyId,
      });
      return;
    }

    if (result.rejected !== undefined || result.dropped !== undefined) {
      this.pending.delete(result.bundleId);
      this.countersFor(entry.strategyId).rejected += 1;
      this.metrics.increment('jitoBundleRejected', { strategy: entry.strategyId });
      this.publishRate(entry.strategyId);
      this.logger.warn('Bundle did not land.', {
        bundleId: result.bundleId,
        strategyId: entry.strategyId,
      });
    }
  }

  private publishRate(strategyId: string): void {
    this.metrics.gauge('jitoBundleLandRate', this.landRate(strategyId), { strategy: strategyId });
  }

  private countersFor(strategyId: string): MutableCounters {
    let counters = this.counters.get(strategyId);
    if (counters === undefined) {
      counters = { submitted: 0, landed: 0, rejected: 0 };
      this.counters.set(strategyId, counters);
    }
    return counters;
  }
}
