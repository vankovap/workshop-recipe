export const NATS_JOBS_SUBJECT = "deck.jobs";
// Queue group for job delivery. Every worker replica subscribes under the same group name, so NATS
// hands each job to exactly one of them instead of fanning it out to all of them.
export const NATS_JOBS_QUEUE_GROUP = "deck-workers";
export const VALKEY_PROGRESS_CHANNEL = "deck:progress";
export const valkeyLockKey = (jobId: string) => `deck:lock:${jobId}`;
export const valkeyProgressKey = (jobId: string) => `deck:progress:${jobId}`;
