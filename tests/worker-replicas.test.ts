import { describe, expect, it } from "vitest";
import {
  createLock,
  createMemoryBus,
  createMemoryCache,
  createMemoryStore,
  handleJob,
} from "@deck/engine";
import { contentHash, splitSlides } from "@deck/shared";

const DECK = "# A\n\n---\n\n# B\n\n---\n\n# C";

async function seedJob(store: ReturnType<typeof createMemoryStore>) {
  const slides = splitSlides(DECK);
  return store.insertJob({
    markdown: DECK,
    contentHash: await contentHash(DECK),
    slideCount: slides.length,
  });
}

describe("multi-replica worker", () => {
  it("hands a published job to exactly one subscriber", async () => {
    const bus = createMemoryBus();
    const seen: string[] = [];
    await bus.subscribe(async () => {
      seen.push("replica-1");
    });
    await bus.subscribe(async () => {
      seen.push("replica-2");
    });

    await bus.publish("job-1");
    await new Promise((resolve) => setTimeout(resolve, 0));

    // Without a queue group NATS fans the job out to every replica, which is what made
    // production render each deck twice.
    expect(seen).toHaveLength(1);
    await bus.close();
  });

  it("keeps progress at the deck length when two replicas render the same job", async () => {
    const store = createMemoryStore();
    const cache = createMemoryCache();
    const job = await seedJob(store);

    // Both replicas picking up one job is the exact production condition: separate processes, so
    // separate in-process locks, and a progress counter they share.
    const replica = (id: string) =>
      handleJob(
        {
          store,
          cache,
          replicaId: id,
          renderDriver: "stub" as const,
          spinMs: 0,
          lock: createLock(),
          log: () => {},
        },
        job.id,
      );

    await replica("w1");
    await replica("w2");

    const done = await store.getJob(job.id);
    expect(done?.status).toBe("done");
    // Pre-fix this was 6 on a three-slide deck, and the UI rendered "6 / 3 rendered".
    expect(await cache.getProgress(job.id)).toBe(3);
  });

  it("never reports progress beyond the deck length under concurrent replicas", async () => {
    const store = createMemoryStore();
    const cache = createMemoryCache();
    const job = await seedJob(store);

    await Promise.all(
      ["w1", "w2", "w3"].map((id) =>
        handleJob(
          {
            store,
            cache,
            replicaId: id,
            renderDriver: "stub" as const,
            spinMs: 0,
            lock: createLock(),
            log: () => {},
          },
          job.id,
        ),
      ),
    );

    expect(await cache.getProgress(job.id)).toBeLessThanOrEqual(3);
  });
});
