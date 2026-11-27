# Example prompts

Natural-language prompts for an LLM agent with the `seestar-mcp` tools
available (`connect`, `status`, `goto`, `park`, `capture`, `solve_field`,
`photometry_run`, `hr_diagram`, `job_status`, `job_result`). All prompts assume the
worker + redis are running and the server was started with
`SEESTAR_MCP_ALLOW_SLEW=1`. Every job takes an `idempotency_key`: reuse the
same key to safely retry a submission, use a fresh key for each new job.
Poll long jobs with `job_status(key)` / `job_result(key)`.

If `goto` fails with RPC code 270, the target is below the horizon at the
scope's location/time — pick another target or wait. Code 207 means the
scope refused the request (e.g. wrong scope clock — call `connect` first,
it syncs the clock).

## 1. Variable star: continuous lightcurve of VW Cep

```markdown
You are running my Seestar S50 tonight via the seestar-mcp tools.
The docker-compose worker and redis are already running,
SEESTAR_MCP_ALLOW_SLEW=1 is set, scope at 192.168.87.185.

1. connect, then status. If goto ever fails with code 270, the target
   is below the horizon — report and stop.
2. goto idempotency_key "vwcep-goto-1", target "VW Cep". Poll
   job_status until SUCCESS; job_result confirms pointing.
3. Observe in a loop until I say stop or the target gets low:
   a. capture idempotency_key "vwcep-cap-{n}" (n = 1,2,3...), project
      "vw-cep", frames 10, exposure 10, gain 80. Poll to SUCCESS before
      the next capture. Note the session id from the first job_result.
   b. Every 5 captures, submit photometry_run idempotency_key
      "vwcep-photo-{m}" (m = 1,2,3...), project "vw-cep", that session id,
      picks = one {"role":"target", x, y} on VW Cep + 2-3
      {"role":"comp", x, y} on similar-brightness field stars (pixel
      coords from the solved frame — run solve_field once on frame 1
      first, key "vwcep-solve-1"). Poll to SUCCESS.
   c. After each photometry run, append only the NEW measurements
      (jd, mag, mag_err, target-minus-comp) to the running lightcurve
      and print the latest magnitude + a one-line trend.
4. On stop: park with idempotency_key "vwcep-park-1", then print the
   full lightcurve table and flag eclipse dips.
```

Each photometry run is cumulative over the whole session, so the agent
diffs old vs new results to update the lightcurve incrementally.

## 2. Open cluster: HR diagram of NGC 7789

```markdown
Use the seestar-mcp tools (worker + redis running, SEESTAR_MCP_ALLOW_SLEW=1).
1. connect, then status to confirm the scope is reachable.
2. goto idempotency_key "ngc7789-goto-1", target "NGC 7789". Poll
   job_status / job_result until the slew completes.
3. capture idempotency_key "ngc7789-cap-1", project "ngc7789", frames 30,
   exposure 10, gain 80. Poll to SUCCESS; note session id + frame ids.
4. solve_field idempotency_key "ngc7789-solve-1", project "ngc7789",
   frame_id of the first captured frame. Poll to SUCCESS; report field
   center + RMS.
5. HR diagram directly: hr_diagram idempotency_key "ngc7789-hr-1",
   project "ngc7789", frame_id of the first captured frame,
   recompute=true (the frame is solved, so the Gaia cross-match can be
   computed and stored), plus age/feh/dist_mod for the isochrone overlay
   (e.g. age 9.0, feh 0.0, dist_mod 11.5 — NGC 7789's rough values).
   Poll to SUCCESS; job_result returns the CMD points (G vs BP-RP), the
   H-R points (abs G vs BP-RP) with cluster-membership flags, and the
   isochrone overlay points. Print the member count and the diagram data.
6. Optionally also run per-star photometry: for each of ~10 bright
   cluster stars s, photometry_run idempotency_key "ngc7789-photo-{s}",
   project "ngc7789", the session id, picks = {"role":"target", x, y} on
   star s plus 3 other cluster stars as {"role":"comp", x, y}
   (photometry_run takes exactly one target). Poll each to SUCCESS.
7. park idempotency_key "ngc7789-park-1".
```

## 3. Tracking quality: drift check over a series of frames

```markdown
Use the seestar-mcp tools (worker + redis running, SEESTAR_MCP_ALLOW_SLEW=1).
1. connect, status. goto idempotency_key "drift-goto-1", target "M13",
   poll to SUCCESS.
2. Capture 10 single frames: capture idempotency_key "drift-cap-{n}"
   (n = 1..10), project "drift-check", frames 1, exposure 10, gain 80.
   Poll each to SUCCESS; collect frame ids + session id.
3. For each frame, solve_field idempotency_key "drift-solve-{n}",
   project "drift-check", frame_id of that capture. Poll to SUCCESS.
4. Report the solved field center (ra, dec) per frame as a table and the
   total drift in arcsec over the series.
5. park idempotency_key "drift-park-1".
```
