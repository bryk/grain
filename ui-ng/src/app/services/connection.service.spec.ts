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
  provideHttpClient,
  withInterceptors,
} from "@angular/common/http";
import {
  HttpTestingController,
  provideHttpClientTesting,
} from "@angular/common/http/testing";
import { TestBed } from "@angular/core/testing";
import { firstValueFrom } from "rxjs";
import {
  ConnectionMonitor,
  NETWORK_REASON,
  SILENT_MS,
  SILENT_REASON,
  connectionInterceptor,
} from "./connection.service";

describe("ConnectionMonitor", () => {
  let http: HttpClient;
  let ctl: HttpTestingController;
  let monitor: ConnectionMonitor;

  beforeEach(() => {
    vi.useFakeTimers();
    TestBed.configureTestingModule({
      providers: [
        provideHttpClient(withInterceptors([connectionInterceptor])),
        provideHttpClientTesting(),
      ],
    });
    http = TestBed.inject(HttpClient);
    ctl = TestBed.inject(HttpTestingController);
    monitor = TestBed.inject(ConnectionMonitor);
  });

  afterEach(() => {
    TestBed.resetTestingModule();
    vi.useRealTimers();
  });

  const send = (url = "/api/tasks") =>
    firstValueFrom(http.get(url)).catch(() => undefined);

  it("raises the silent banner when a request goes unanswered, then clears on an answer", () => {
    void send();
    vi.advanceTimersByTime(SILENT_MS / 2);
    // Halfway there a probe goes out; it is held too.
    const probe = ctl.expectOne("/api/whoami");
    expect(monitor.disconnection()).toBeNull();
    vi.advanceTimersByTime(SILENT_MS / 2);
    expect(monitor.disconnection()?.reason).toBe(SILENT_REASON);
    probe.flush({});
    expect(monitor.disconnection()).toBeNull();
    ctl.expectOne("/api/tasks").flush([]);
  });

  it("does not count a slow request whose probe is answered", () => {
    void send();
    vi.advanceTimersByTime(SILENT_MS / 2);
    ctl.expectOne("/api/whoami").flush({});
    vi.advanceTimersByTime(SILENT_MS * 2);
    expect(monitor.disconnection()).toBeNull();
    ctl.match("/api/whoami").forEach((r) => r.flush({}));
    ctl.expectOne("/api/tasks").flush([]);
  });

  it("needs a second refusal before showing a failed connection", () => {
    void send();
    ctl
      .expectOne("/api/tasks")
      .error(new ProgressEvent("error"), { status: 0 });
    expect(monitor.disconnection()).toBeNull();
    ctl
      .expectOne("/api/whoami")
      .error(new ProgressEvent("error"), { status: 0 });
    expect(monitor.disconnection()?.reason).toBe(NETWORK_REASON);
  });

  it("counts an error the daemon itself sent as an answer", () => {
    void send();
    ctl
      .expectOne("/api/tasks")
      .error(new ProgressEvent("error"), { status: 0 });
    void send("/api/config");
    ctl
      .expectOne("/api/config")
      .flush({ error: "nope" }, { status: 500, statusText: "Server Error" });
    ctl.match("/api/whoami").forEach((r) => r.flush({}));
    void send();
    ctl
      .expectOne("/api/tasks")
      .error(new ProgressEvent("error"), { status: 0 });
    expect(monitor.disconnection()).toBeNull();
  });

  it("treats a gateway's 502 as unreachable", () => {
    void send();
    ctl
      .expectOne("/api/tasks")
      .flush("bad gateway", { status: 502, statusText: "Bad Gateway" });
    ctl
      .expectOne("/api/whoami")
      .flush("bad gateway", { status: 502, statusText: "Bad Gateway" });
    expect(monitor.disconnection()?.reason).toContain("502 Bad Gateway");
  });

  it("ignores requests outside /api/", () => {
    void send("/next/assets/x.json");
    vi.advanceTimersByTime(SILENT_MS * 2);
    expect(monitor.disconnection()).toBeNull();
    ctl.expectOne("/next/assets/x.json").flush({});
  });
});
