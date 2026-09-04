import { EventEmitter } from "node:events";
import { connect, JSONCodec } from "nats";
import { NATS_JOBS_QUEUE_GROUP, NATS_JOBS_SUBJECT } from "@deck/shared";

export type JobHandler = (jobId: string) => Promise<void>;

export interface Bus {
  publish(jobId: string): Promise<void>;
  subscribe(handler: JobHandler): Promise<void>;
  close(): Promise<void>;
}

type JobMessage = { jobId: string };

export function createMemoryBus(): Bus {
  const events = new EventEmitter();
  events.setMaxListeners(50);
  // The NATS bus delivers each job to one member of the queue group. This stand-in round-robins for
  // the same reason: a test with two subscribers must see one render, not one per subscriber.
  const handlers: JobHandler[] = [];
  let next = 0;
  events.on(NATS_JOBS_SUBJECT, (jobId: string) => {
    const handler = handlers[next % handlers.length];
    if (!handler) return;
    next += 1;
    void handler(jobId);
  });
  return {
    async publish(jobId) {
      events.emit(NATS_JOBS_SUBJECT, jobId);
    },
    async subscribe(handler) {
      handlers.push(handler);
    },
    async close() {
      handlers.length = 0;
      events.removeAllListeners();
    },
  };
}

export type NatsConnect = {
  servers: string;
  user?: string;
  pass?: string;
};

export function natsFromEnv(
  env: NodeJS.ProcessEnv = process.env,
): string | NatsConnect | undefined {
  if (env.NATS_HOST) {
    return {
      servers: `${env.NATS_HOST}:${env.NATS_PORT ?? "4222"}`,
      user: env.NATS_USER,
      pass: env.NATS_PASSWORD ?? env.NATS_PASS,
    };
  }
  if (env.NATS_URL) return env.NATS_URL;
  return undefined;
}

/** Subject for job messages. NATS_SUBJECT lets a dev deployment share the broker with stage on its own subject. */
export function natsSubjectFromEnv(env: NodeJS.ProcessEnv = process.env): string {
  return env.NATS_SUBJECT?.trim() || NATS_JOBS_SUBJECT;
}

export async function createNatsBus(
  input: string | NatsConnect,
  subject: string = natsSubjectFromEnv(),
): Promise<Bus> {
  const opts = typeof input === "string" ? { servers: input } : input;
  const nc = await connect(opts);
  const codec = JSONCodec<JobMessage>();
  return {
    async publish(jobId) {
      nc.publish(subject, codec.encode({ jobId }));
    },
    async subscribe(handler) {
      const sub = nc.subscribe(subject, { queue: NATS_JOBS_QUEUE_GROUP });
      void (async () => {
        for await (const msg of sub) {
          const { jobId } = codec.decode(msg.data);
          await handler(jobId);
        }
      })();
    },
    async close() {
      await nc.drain();
    },
  };
}
