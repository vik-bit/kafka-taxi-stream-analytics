# LLM usage

Coding assistant: Grok 4.7 in Cursor. It was used to turn the assignment PDF into this pipeline, including the stream stages, the sketches, the experiment runner, and the write-up of the measured numbers.

## Five prompts that changed the implementation

1. Build the assignment from the PDF as one pipeline. Do not skip a required topic, metric, output field, or experiment. Kafka must sit between every stage after the producer.
2. Implement DGIM with the usual rules: at most two buckets of each size, timestamp of the newer bucket on a merge, and a half-count for the oldest bucket that overlaps the query. Check it against an exact deque for k = 1000, 5000, and 10000.
3. Keep Bloom, the reservoirs, HyperLogLog, and DGIM correct when the producer resends an `event_id`. Never drop an event only because the Bloom filter says it was seen. Report a real false-positive example, shrinking the filter if the default run has none.
4. Run P1–P4, pickup-zone vs round-robin, and low/medium/high rate as separate Kafka runs. Recreate `taxi.raw` and `taxi.clean` between runs so lag and offsets do not leak.
5. Make the late-event watermark and a Stage 2 kill/restart observable: one included event, one discarded event, and the lag and committed offset before the kill, while Stage 2 is down, and after it resumes.

## A generated approach that was wrong

Two pieces of the first end-to-end runner failed on this machine.

Status files were written with an atomic replace inside the project directory. The folder is on OneDrive, and Windows denied the read while the replace was in progress (`PermissionError` on `results/control/P1_stage1.json`). Stage 2 and Stage 3 died mid-run, and the final consumer then sat until it was killed. Hot status files now live in the OS temp directory, and reads retry on a sharing violation.

The restart path had a second bug. "Lag is zero" was treated as end-of-stream even when the new process had not been assigned a partition yet. An unassigned consumer reports lag 0, so the restarted Stage 2 exited immediately, left the backlog on `taxi.clean`, and never resumed. The exit condition now requires a real assignment before lag 0 counts as caught up. After that fix, the recorded restart drains the suffix and lag returns to 0.

A related measurement was also misleading: counting idle consumers from the assignment map after the threads had been told to stop. During that shutdown the group rebalanced and one thread briefly appeared to hold every partition, so 6 consumers on 4 partitions looked fully busy. The idle count is now the median number of threads that held a partition while the run was live. P4 shows 4 assigned and 2 idle.

## What I can explain without the assistant

I can walk the path of one trip from the CSV into `taxi.raw`, through cleaning and the Bloom check, into a 60-minute window, and out to one line of `results.jsonl`. I can explain why a Bloom hit must not drop the event, why DGIM's oldest bucket is counted as half, why 10×10 banding recalls almost none of the pairs at Jaccard 0.50 while 20×5 recalls about three quarters, and why two of the six consumers in P4 sit idle. I can also say what the restart numbers mean: committed offset frozen, lag up, then a new process that only sees the suffix because the sketches were only in memory.
