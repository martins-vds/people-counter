# Stable SJD getting-started demo tutorial

This tutorial is the presenter runbook for
[`18_sjd_getting_started_demo.ipynb`](./18_sjd_getting_started_demo.ipynb).
The notebook contains only operational configuration and execution cells; this
document contains the presentation order, narration, checkpoints, and fallback
guidance.

## What the demo proves

The demo processes three reviewed OneLake videos through the stable Spark Job
Definitions and publishes the results to the SJD-native Power BI report:

1. immutable work registration;
2. atomic, runtime-compatible batch claiming;
3. executor-partition video processing;
4. committed-pointer gold publication;
5. Direct Lake semantic-model framing; and
6. analytics and capacity-forecast visualization.

The sample workload totals approximately 106.86 seconds, or 0.03 video-hours.
It uses the reviewed `onnx-r18-b1-1fps-1t` profile.

## Before the audience arrives

1. Import or open the notebook in the `people-counter-dev` workspace.
2. Attach the `people_counter_dev` Lakehouse and the published people-counter
   Environment.
3. Run the parameter, reviewed-input, and helper cells with every side-effect
   gate left `False`.
4. Record the generated `RUN_LABEL`. Reuse it if the notebook session must be
   restarted.
5. Set `REGISTER_INPUTS=True` and run the registration cell.
6. Wait for all three registration jobs to complete.
7. Return `REGISTER_INPUTS` to `False`.
8. Leave the report open on **Video processing** in a separate browser tab.

Registration should be completed before the live presentation because each of
the three invocations includes Fabric Spark startup time. The live claim,
process, and gold stages also include startup time, so plan the presentation
around visible job-status polling.

## 1. Introduce the workload

Show the three reviewed inputs and their total duration in the notebook.

Say:

> “These three videos are stored in OneLake and registered with immutable
> source and model hashes. Together they represent about 107 seconds of video.
> The demo uses the reviewed ONNX RT-DETR R18 batch-one CPU profile.”

Do not expose access tokens or copy token-bearing HTTP headers into the
presentation. The notebook obtains short-lived tokens through
`notebookutils.credentials`.

## 2. Explain registration

Show the completed registration output and the three generated work IDs.

Say:

> “Registration creates immutable work identities and records the expected
> source, model, configuration, and release digests. A registration retry is
> accepted only when the work ID and payload still match.”

Expected checkpoint:

- all three control jobs are `Completed`;
- each work ID is present in `people_counter_sjd_work`; and
- each work item is eligible to be claimed.

If registration failed, do not continue to claim. Read the failed job state,
correct the cause, and retry with the same `RUN_LABEL` only when the payload is
unchanged.

## 3. Claim one batch

Set `CLAIM_BATCH=True`, run the claim cell, and wait for the control job to
complete. Return the gate to `False` afterward.

Say while the job runs:

> “The control job atomically claims exactly these three compatible videos and
> seals an immutable batch envelope. Another worker cannot process the same
> attempts concurrently.”

Show the resulting batch row. Point out:

- `item_count` is 3;
- the owner matches the generated demo owner;
- the batch ID is captured automatically for the next stage; and
- the batch status is consistent with a sealed, processable claim.

If the notebook session is restarted after this point, preserve the original
`RUN_LABEL` and copy the printed batch ID into `RESUME_BATCH_ID`.

## 4. Process the videos

Set `PROCESS_BATCH=True`, run the process cell, and wait for the Spark job to
complete. Return the gate to `False` afterward.

Say:

> “Spark distributes whole videos to executor partitions. Each attempt is
> staged, validated, and independently published with release provenance,
> processing time, people counts, and retryable failure details.”

While polling, explain that Spark startup is separate from inference time. The
report's `processing_seconds` measures video processing rather than the full
job wall-clock duration.

Expected checkpoint:

- the process job is `Completed`;
- no work ID is reported as failed;
- committed attempt pointers exist for all three videos; and
- terminal video summaries include processing time and distinct-person count.

Do not start a second process job for the batch merely because startup is slow.
The processing contract is resumable and retry-safe, but overlapping manual
invocations make the demonstration harder to follow.

## 5. Publish stable gold tables

Set `BUILD_GOLD=True`, run the gold cell, and wait for completion. Return the
gate to `False` afterward.

Say:

> “The gold job reads only committed attempt pointers and incrementally
> publishes the stable `people_counter_sjd_gold_*` facts and dimensions. It
> does not treat uncommitted staging records as reportable results.”

Expected checkpoint:

- the gold job is `Completed`; and
- the three work IDs appear in `people_counter_sjd_gold_video`.

## 6. Frame the Direct Lake model

Set `REFRESH_MODEL=True`, run the refresh cell, and wait for `Completed`.
Return the gate to `False` afterward.

Say:

> “The physical gold rows are ready. This refresh frames the Direct Lake
> semantic model so the report sees the latest Delta-table state.”

The framing operation normally completes much faster than the Spark jobs. Do
not refresh the browser report until the notebook reports `Completed`.

## 7. Verify the physical results

Set `VERIFY_RESULTS=True` and run the verification cell.

Say:

> “The processing results are visible through the stable reporting schema. We
> can inspect per-video duration, processing time, real-time speed, and
> distinct-person counts without querying staging data.”

Confirm that:

- exactly three rows are displayed;
- the source durations total approximately 106.86 seconds;
- `processing_seconds` and `speed_x_realtime` are populated;
- `distinct_people` is populated; and
- `completed_at` is populated.

## 8. Present the Power BI report

Open the printed report URL and reload **Video processing**. Show:

- the three new work IDs;
- approximately 0.03 additional video-hours;
- processing seconds and real-time speed for each video;
- distinct-person counts; and
- the `onnx-r18-b1-1fps-1t` configuration.

Then open **Forecast** and say:

> “These capacity estimates use our measured concurrent F64 baseline. They are
> directional, not guarantees, and explicitly call out nonlinear scaling,
> throttling, startup time, retries, I/O, model-cache behavior, and workload
> contention.”

The **Video hours to process** selector defaults to **200,000**. Change it to
the customer's expected workload to update the required-throughput cards and
estimated completion days. The comparison table includes F64, F128, F256,
F512, F1024, F2048, F4096, and F8192.

Explain that the experimental single-video ONNX result is shown for context but
does not replace the concurrent F64 planning baseline.

## Traffic-page limitation

Do not promise entry or exit values during this demo. Flow facts are sparse and
appear only when configured-line crossing events are emitted. Successful
inference and populated distinct-person metrics do not imply that a tracked
person crossed the configured counting line.

If the Traffic page is empty, say:

> “The pipeline completed successfully, but this run did not emit a qualifying
> line-crossing event. The Video processing page is the authoritative view for
> this demonstration.”

## Completion checklist

- [ ] Three registrations completed before the live portion.
- [ ] One three-item batch was claimed.
- [ ] The process job completed without failed work IDs.
- [ ] The gold job completed.
- [ ] The semantic-model refresh completed.
- [ ] Exactly three stable gold rows were verified.
- [ ] The Video processing page showed the new results.
- [ ] The Forecast assumptions and warnings were explained.
- [ ] No unsupported Traffic-page result was promised.
