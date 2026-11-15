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
  ChangeDetectionStrategy,
  Component,
  ViewEncapsulation,
  computed,
  inject,
} from "@angular/core";
import { ConnectionMonitor } from "../services/connection.service";

// The shell's disconnected banner, the classic UI's DisconnectedBanner:
// every request to the daemon has gone unanswered (ConnectionMonitor), so
// whatever the page shows is however old "since" says, not current. It
// sits above the stage and the Director drawer, outside the subtree a
// modal makes inert, so a reader working in a dialog still hears it.
//
// The live region is always in the page and only its text comes and
// goes: a region that already exists when it changes is what a screen
// reader reliably announces. role="status" rather than "alert" -- it is
// news, not an interruption -- and it says when, not how long ago, so it
// is not re-announced every minute of the outage. It takes itself down
// on the first answer, so it has no close button.
@Component({
  selector: "g-disconnected-banner",
  encapsulation: ViewEncapsulation.None,
  changeDetection: ChangeDetectionStrategy.OnPush,
  styles: [
    `
      g-disconnected-banner {
        display: block;
        flex: none;
        min-width: 0;
      }
      .disconnected-banner {
        display: flex;
        align-items: flex-start;
        gap: 8px;
        padding: 8px 20px;
        background: var(--md-sys-color-warning-container);
        color: var(--md-sys-color-on-warning-container);
        border-bottom: 1px solid var(--md-sys-color-warning);
        font: 500 13px/18px var(--grain-font-body);
        font-variant-numeric: tabular-nums;
        overflow-wrap: anywhere;
      }
      .disconnected-banner svg {
        flex: none;
        margin-top: 1px;
      }
      .disconnected-banner-copy {
        min-width: 0;
      }
      @media (max-width: 599px) {
        .disconnected-banner {
          padding: 8px 12px;
        }
      }
    `,
  ],
  template: `
    <div role="status" data-testid="disconnected-region">
      @if (text(); as t) {
        <div class="disconnected-banner" data-testid="disconnected-banner">
          <svg width="16" height="16" viewBox="0 0 24 24" aria-hidden="true">
            <path
              d="M12 2L1 21h22L12 2zm1 14h-2v2h2v-2zm0-6h-2v4h2v-4z"
              fill="currentColor"
            />
          </svg>
          <span class="disconnected-banner-copy">
            <strong>{{ t.reason }}</strong>
            {{ t.rest }}
          </span>
        </div>
      }
    </div>
  `,
})
export class DisconnectedBannerComponent {
  private readonly monitor = inject(ConnectionMonitor);

  protected readonly text = computed(() => {
    const d = this.monitor.disconnection();
    if (!d) return null;
    const at = new Date(d.since).toLocaleTimeString([], {
      hour: "numeric",
      minute: "2-digit",
    });
    return {
      reason: d.reason,
      rest: `Unreachable since ${at} — retrying automatically; this page will pick back up on its own once it answers again.`,
    };
  });
}
