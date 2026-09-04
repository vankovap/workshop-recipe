import { splitSlides, type JobEvent } from "@deck/shared";
import type { Cache } from "./cache.js";
import { processLock, type JobLock } from "./lock.js";
import { renderAllSlides, slidesToPdf, type RenderDriver } from "./render.js";
import type { Store } from "./store.js";

export type HandleDeps = {
  store: Store;
  cache: Cache;
  replicaId: string;
  renderDriver: RenderDriver;
  spinMs?: number;
  lock?: JobLock;
  log?: (line: string) => void;
};

export async function handleJob(deps: HandleDeps, jobId: string): Promise<void> {
  const log = deps.log ?? console.log;
  const lock = deps.lock ?? processLock;
  const acquired = lock.tryAcquire(jobId);
  if (!acquired) {
    log(`skip, already rendering locally job=${jobId} replica=${deps.replicaId}`);
    return;
  }
  log(`acquired local render lock job=${jobId} replica=${deps.replicaId}`);

  try {
    const job = await deps.store.getJob(jobId);
    if (!job) {
      log(`job missing job=${jobId} replica=${deps.replicaId}`);
      return;
    }
    await deps.store.updateStatus(jobId, "rendering");
    const slides = splitSlides(job.markdown);
    const spinMs = deps.spinMs ?? Number(process.env.RENDER_SPIN_MS ?? 400);
    const pngs = await renderAllSlides(slides, deps.renderDriver, spinMs);
    for (const [index, png] of pngs.entries()) {
      const write = await deps.store.putSlide(jobId, index, png, deps.replicaId);
      if (write === "conflict") {
        const detail = `duplicate slide persist job=${jobId} replica=${deps.replicaId} index=${index}`;
        log(detail);
        const conflict: JobEvent = {
          type: "job.conflict",
          jobId,
          replicaId: deps.replicaId,
          detail,
        };
        await deps.cache.publishEvent(conflict);
        // The progress key is shared by every replica, so a slide that someone else already persisted
        // has already been counted. Counting it again is what drove progress past the deck length
        // ("Slide 6 / 3" on a three-slide deck), so a conflicting write must not advance it.
        continue;
      }
      const current = await deps.cache.incrProgress(jobId);
      const progress: JobEvent = {
        type: "job.progress",
        jobId,
        current,
        total: slides.length,
        replicaId: deps.replicaId,
      };
      await deps.cache.publishEvent(progress);
    }
    await deps.store.putPdf(jobId, await slidesToPdf(pngs));
    await deps.store.updateStatus(jobId, "done");
    await deps.cache.publishEvent({ type: "job.done", jobId });
    log(`render complete job=${jobId} replica=${deps.replicaId} slides=${slides.length}`);
  } catch (err) {
    log(`render failed job=${jobId} replica=${deps.replicaId} error=${(err as Error).message}`);
    await deps.store.updateStatus(jobId, "failed");
  } finally {
    lock.release(jobId);
  }
}
