// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import {
  HttpClient,
  HttpErrorResponse,
  HttpHeaders,
  type HttpInterceptorFn,
  HttpResponse,
} from "@angular/common/http";
import { Injectable, NgZone, OnDestroy, inject, signal } from "@angular/core";
import { finalize, firstValueFrom, tap, timeout } from "rxjs";

// Disconnection is what the shell's disconnected banner shows: every
// recent request to the daemon has gone unanswered, as opposed to the
// daemon answering one with a problem. `since` is when the silence (or
// the first failure) began, not when the shell noticed it.
export interface Disconnection {
  reason: string;
  since: number;
}

// How long a request may go unanswered, with nothing else answering
// either, before the daemon counts as accepting connections but sending
// nothing (a wedged daemon, a stalled proxy) -- the classic UI's
// CONFIG_POLL_TIMEOUT_MS.
export const SILENT_MS = 10_000;
// How often the watchdog looks at what is outstanding, and how often a
// liveness probe is sent while something looks wrong.
const WATCH_MS = 1_000;
export const PROBE_MS = 5_000;
// Failed requests in a row, with no answer between them, before a
// refused connection counts: one dropped request during a deploy's
// endpoint shuffle is not an outage.
const FAILURES_TO_SHOW = 2;

export const NETWORK_REASON =
  "This deployment cannot be reached: the connection to it failed.";
export const SILENT_REASON = `This deployment cannot be reached: it accepted the connection but sent no answer within ${SILENT_MS / 1000} seconds.`;

function gatewayReason(status: number): string {
  if (status === 502) {
    return "This deployment cannot be reached: a gateway in front of it reports it is down (502 Bad Gateway).";
  }
  if (status === 503) {
    return "This deployment cannot be reached: a gateway in front of it reports it is unavailable (503 Service Unavailable).";
  }
  return "This deployment cannot be reached: a gateway in front of it timed out waiting for an answer (504 Gateway Timeout).";
}

// isGatewayError says whether an error status is a proxy speaking for a
// daemon that is not there, rather than the daemon itself. The daemon
// always answers in JSON (its own 503, verifier_unavailable, included);
// a gateway's error page is not.
function isGatewayError(err: HttpErrorResponse): boolean {
  if (err.status !== 502 && err.status !== 503 && err.status !== 504) {
    return false;
  }
  const body = err.error;
  if (body && typeof body === "object") return false;
  if (typeof body === "string" && body.trim().startsWith("{")) return false;
  return true;
}

// ConnectionMonitor is the shell's view of whether the daemon is
// answering at all. connectionInterceptor reports every /api/ request to
// it; it raises `disconnection` when requests are refused (status 0 or a
// gateway's 502/503/504) twice running, or when one has been outstanding
// for SILENT_MS with nothing answering meanwhile -- the accepted-but-
// silent daemon a fetch failure alone never reports. While anything looks
// wrong it sends its own bounded probe every PROBE_MS, so the banner
// clears on the first answer even on a page that polls nothing; any
// answer at all, from any request, clears it.
@Injectable({ providedIn: "root" })
export class ConnectionMonitor implements OnDestroy {
  private readonly zone = inject(NgZone);
  // Optional so a shell rendered without HTTP (unit tests) still mounts
  // the banner; it then just never probes.
  private readonly http = inject(HttpClient, { optional: true });

  readonly disconnection = signal<Disconnection | null>(null);

  private readonly pending = new Map<number, number>();
  private nextId = 1;
  private lastAnswerAt = Date.now();
  private failures = 0;
  private firstFailureAt = 0;
  private watchTimer: ReturnType<typeof setInterval> | null = null;
  private lastProbeAt = 0;
  private probing = false;

  ngOnDestroy(): void {
    this.stopWatch();
  }

  // started records a request leaving; the id it returns is handed back
  // to settled when it ends, however it ends.
  started(now = Date.now()): number {
    const id = this.nextId++;
    this.pending.set(id, now);
    this.startWatch();
    return id;
  }

  settled(id: number): void {
    this.pending.delete(id);
  }

  answered(now = Date.now()): void {
    this.lastAnswerAt = now;
    this.failures = 0;
    this.firstFailureAt = 0;
    if (this.disconnection() !== null)
      this.zone.run(() => this.disconnection.set(null));
    if (this.pending.size === 0) this.stopWatch();
  }

  failed(reason: string, sentAt: number, now = Date.now()): void {
    if (this.failures === 0) this.firstFailureAt = sentAt;
    this.failures++;
    if (this.failures >= FAILURES_TO_SHOW) {
      this.raise(reason, this.firstFailureAt);
    } else {
      this.probe(now);
    }
    this.startWatch();
  }

  // check is the watchdog's tick, public so a test can drive it.
  check(now = Date.now()): void {
    let oldest = Infinity;
    for (const at of this.pending.values()) oldest = Math.min(oldest, at);
    const unanswered = oldest !== Infinity && this.lastAnswerAt <= oldest;
    // A request that is merely slow (a big diff, a long POST) is not an
    // outage: halfway to SILENT_MS a probe goes out, and its answer
    // counts as one, so only a daemon silent to both raises the banner.
    if (unanswered && now - oldest >= SILENT_MS) {
      this.raise(SILENT_REASON, oldest);
    }
    if (
      this.disconnection() !== null ||
      this.failures > 0 ||
      (unanswered && now - oldest >= SILENT_MS / 2)
    ) {
      this.probe(now);
    }
    if (
      this.pending.size === 0 &&
      this.disconnection() === null &&
      this.failures === 0
    ) {
      this.stopWatch();
    }
  }

  private raise(reason: string, since: number): void {
    if (this.disconnection() !== null) return;
    this.zone.run(() => this.disconnection.set({ reason, since }));
  }

  private probe(now: number): void {
    if (this.probing || now - this.lastProbeAt < PROBE_MS || !this.http) {
      return;
    }
    this.probing = true;
    this.lastProbeAt = now;
    void firstValueFrom(
      this.http
        .get("/api/whoami", {
          headers: new HttpHeaders({ "Content-Type": "application/json" }),
        })
        .pipe(timeout(SILENT_MS)),
    )
      .catch(() => undefined)
      .finally(() => {
        this.probing = false;
      });
  }

  private startWatch(): void {
    if (this.watchTimer) return;
    this.zone.runOutsideAngular(() => {
      this.watchTimer = setInterval(() => this.check(), WATCH_MS);
    });
  }

  private stopWatch(): void {
    if (!this.watchTimer) return;
    clearInterval(this.watchTimer);
    this.watchTimer = null;
  }
}

// connectionInterceptor tells ConnectionMonitor about every /api/
// request: when it left, whether the daemon answered (any status it sent
// itself, 4xx and 5xx included) or it was refused, and when it ended --
// including being cancelled, which leaves no verdict either way.
export const connectionInterceptor: HttpInterceptorFn = (req, next) => {
  const path = req.url.replace(/^https?:\/\/[^/]+/, "");
  if (!path.startsWith("/api/")) return next(req);
  const monitor = inject(ConnectionMonitor);
  const sentAt = Date.now();
  const id = monitor.started(sentAt);
  return next(req).pipe(
    tap({
      next: (ev) => {
        if (ev instanceof HttpResponse) monitor.answered();
      },
      error: (err: unknown) => {
        if (!(err instanceof HttpErrorResponse)) return;
        if (err.status === 0) monitor.failed(NETWORK_REASON, sentAt);
        else if (isGatewayError(err)) {
          monitor.failed(gatewayReason(err.status), sentAt);
        } else monitor.answered();
      },
    }),
    finalize(() => monitor.settled(id)),
  );
};
