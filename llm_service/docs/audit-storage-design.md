# Audit storage: design

## Goal

The audit log stays in blob storage, and the service stays fast when the log grows. A restart takes seconds. The audit table and the usage totals read no prompts. Prompts have their own files and their own retention.

## The problem today

The service writes one file for each flush, every 5 seconds under load. Each record holds the full prompt. Blob storage has no append, and each read costs about 0.3 seconds.

- A busy day makes about 17,000 files.
- A start reads every audit file of the current day to build the usage totals.
- The audit table, the user list and the usage totals read all prompts, and they need none.
- The service keeps the files it read in memory, with the prompts.

## Requirements

1. A start reads a fixed, small number of files for the usage totals, for any load on the current day.
2. A list of audit records reads no prompt.
3. The service reads a prompt only when an admin opens one record.
4. A finished day is one metadata file.
5. A failure in the middle of a write or a merge loses no record and counts no record twice.
6. The files of the current layout stay readable.
7. An admin can set a retention time for prompts that is shorter than the retention of the history.

## Layout

All paths are under `LLM_BLOB_ROOT`.

```
audit/<day>/segments/<stamp>-<id>.jsonl    metadata rows of one flush
audit/<day>/parts/<bucket>.jsonl           metadata rows of one 10 minute bucket
audit/<day>/day.jsonl                      metadata rows of one finished day
prompts/<day>/<stamp>-<id>.jsonl           prompts of one flush
usage/<day>.json                           usage totals of a finished day (exists today)
usage/<day>.partial.json                   usage totals of the current day
audit/<day>/<name>.jsonl                   old layout: full records, read only
```

- `<day>` is the UTC day of the record time `ts`, for example `2026-11-11`.
- `<stamp>` is the UTC time of the flush with the date: `20261111T140359.123456`. Within one run of the service, a later flush always has a greater stamp, also when the clock steps back. If the clock gives an earlier time, the service adds one microsecond to the last stamp. After a restart, the stamp follows the clock. See [Limits](#limits).
- `<id>` is a random hex text. `<stamp>-<id>` is the segment name.
- `<bucket>` is the first 12 characters of the stamp: date, hour and the tens digit of the minute, for example `20261111T140`.

Prompts live under a prefix of their own. A listing of `audit/` never returns a prompt file.

## Records

### Metadata row

A metadata row is the audit record of today without the prompt, plus four fields:

| Field | Value |
|---|---|
| `seg` | The segment name that first held the row. A merge keeps it. |
| `event` | `model_switch`, `engine_reload`, `model_delete`, or null for a request. |
| `detail` | For an event: the small event data (`revision`, `revisions`, `error`). Null for a request. |
| `prompt_ref` | For a request: `{"file": <segment name>, "line": <index>}`. Null for an event. |

The other fields stay: `ts`, `request_id`, `user`, `endpoint`, `model`, `stream`, `status`, `prompt_tokens`, `completion_tokens`, `total_tokens`, `duration_ms`.

### Prompt line

One line for each request record of the flush, in the order of the rows:

```json
{"request_id": "...", "prompt": {"messages": [...]}}
```

`prompt_ref.line` is the index of the line in its file. The reader checks that the `request_id` of the line matches.

## Writes

A flush handles the records of each day in this order:

1. Write the prompt file `prompts/<day>/<segment>.jsonl`. Skip this step if the flush has no request record for the day.
2. Write the segment file `audit/<day>/segments/<segment>.jsonl`.

The segment file is the commit. A record exists when its metadata row exists. If step 1 fails, the flush keeps the records and tries again later with a new segment name. If step 2 fails, the same happens, and the prompt file of step 1 stays as an orphan that nothing reads.

After step 2, the flush puts the rows of the segment into the memory cache. The service never reads back a segment that it wrote.

A record that `json.dumps` cannot write is dropped with a warning that names its `request_id`. It cannot block later flushes.

A failure can happen after a blob was written, for example when the put succeeds and the answer is lost. The records then stay in the buffer. The next flush writes them again under a new segment name, so they appear two times. See [Limits](#limits).

The flush interval and the flush size stay as they are: 5 seconds and 100 records.

## Merges

The flusher thread runs the merge after a flush. The merge runs at most one time each minute. It holds the flush lock, so no flush runs during a merge.

The merge lists only the day folders that have work. The first merge after a start lists `audit/` one time and notes each folder that has a segment or a part. After that, each flush adds its day to the set. A folder leaves the set when it holds no segment and no part after a merge.

A bucket merges 1 to 2 minutes after its end, and a day merges 15 to 16 minutes after its end. The wait comes from the delay of the rules below plus the one minute between two merges.

The last flusher cycle at a stop does not merge. A stop of the service waits for a merge that runs at that moment.

### Parts

A bucket is ready when its 10 minutes ended at least 1 minute ago. For each ready bucket of each day folder in the set:

1. Read the segments of the bucket in name order. Most come from the memory cache.
2. Write `audit/<day>/parts/<bucket>.jsonl` with all rows.
3. Delete the segments of the bucket.

### Day files

A day is ready when it ended at least 15 minutes ago, it has at least one part, and it has no segment left. For each ready day:

1. Read the parts in name order.
2. Write `audit/<day>/day.jsonl`.
3. Delete the parts.

A late record can still arrive in a day folder that has a day file. It makes a new segment, then a new part. The next merge writes the day file again with the old rows and the new rows, then deletes the part. The day file is the only file that the service writes more than one time.

### Failures

- A failed write in step 2 changes nothing. The merge runs again at the next cycle.
- A stop between step 2 and step 3 leaves the merged file and its inputs. The reader handles this with the rule in [Reads](#reads), and the next merge deletes the inputs.
- A merge never changes a row, so a row keeps its `seg`.
- A folder name that is not a day, or a file name that is not a bucket, has no end time. The merge skips it and logs one warning.
- The merges assume that a blob write is atomic. A reader never sees half a file.

## Reads

### Which files count

For one day folder, the reader takes the files in this order. It skips a row whose `seg` occurred in an earlier file of the list:

1. `day.jsonl`, if it exists.
2. Each part.
3. Each segment.
4. Each file of the old layout.

The `seg` check makes a row count one time when a merged file and its inputs both exist. A merge can delete an input between the listing and the read of a reader. The reader then lists the day again. It lists at most 3 times in all, then it reads the files that exist and logs a warning. An old layout file has no `seg`. The reader uses the file name as the `seg` of its rows.

### Old layout

A file directly in `audit/<day>/` holds full records. The reader builds metadata rows from it: it moves `prompt.event` to `event`, the rest of an event prompt to `detail`, and it sets `prompt_ref` to the file and the line. It reads the prompt from the same file when an admin opens the record. The service does not merge or change these files.

### Memory cache

The cache holds metadata rows by file path. It holds no prompts. Its limit is a row count, `ROW_CACHE_SIZE = 1,000,000`, and it drops the least recently used file first. A merge puts the merged file into the cache and removes its inputs. When the service writes a day file again, it replaces the cache entry. A file with more rows than the whole limit is not cached.

A small second cache holds the last 32 prompt files that an admin opened.

### Records in the buffer

Records that wait for a flush appear in lists as today. Their prompts are in memory, so the service answers a record request for them from the buffer.

## Usage totals

### Checkpoint of the current day

The usage totals keep a second set of numbers for the current day: the totals of the rows that a flush wrote. After each successful flush, the audit log passes the written rows and the segment name to the usage totals. The usage totals add the rows to this set and remember the segment name as `through`.

The flusher writes `usage/<day>.partial.json` for each day whose flushed totals changed. It writes at most one time each 60 seconds, and one more time at a clean stop:

```json
{"day": "2026-11-11", "through": "20261111T140359.123456-ab12...", "rows": [...]}
```

The rows have the form of the daily file. The checkpoint holds only flushed rows, so it never counts a record that a crash loses. A partial file that is not valid is ignored with a warning, and the day is read in full.

### Start

For each day folder:

1. If `usage/<day>.json` exists, use it. This is today's behavior.
2. Otherwise, if `usage/<day>.partial.json` exists, use its rows. Then read the metadata of the day and add each row whose `seg` is greater than `through`.
3. Otherwise, read all metadata of the day.

Step 2 reads the files that hold rows after `through`. In the worst case these are the segments of the last minute and one part. A day that got a late record adds its day file. It never reads files of the old layout, because they are older than every segment. The rows of old files count in the totals through step 3 or through the partial file.

### Finished days

When a day is finished, the service writes `usage/<day>.json` as today and deletes the partial file.

## Prompt retention

A new deploy variable `LLM_PROMPT_RETENTION_DAYS`. It is not set by default, and the service then keeps all prompts.

The value is a whole number from 1 to 36500. Any other value stops the service at the start, with an error that names the variable.

When it is set to N, the flusher deletes each folder `prompts/<day>/` whose day ended more than N days ago. The test is strict. A day that ended exactly N days ago stays. The check runs one time each hour. One flusher cycle handles at most 20 days, and the next cycle goes on with the rest. The last cycle at a stop skips retention. The metadata stays.

The flusher takes the day names from the day folders of `audit/`. It never lists all of `prompts/`. It remembers the days that it cleaned, so each hour it lists only the days that newly passed the limit.

A record whose prompt file is gone answers with `"prompt": null` and `"prompt_deleted": true`. The answer has `prompt_deleted` only in this case.

Old layout files hold metadata and prompts together. Retention does not touch them.

## API changes

### `GET /admin/audit`

Each record in `data` is a metadata row without `seg` and `prompt_ref`. It has `event` and `detail`, and no `prompt` field. The filters, the order and the paging stay. `since` and `until` accept a day or an ISO 8601 time, as before.

### `GET /admin/audit/records/{request_id}?day=YYYY-MM-DD`

New. It returns one full record: the fields of the list row plus `prompt`. So the record has `event` and `detail` too. For an event, `prompt` holds `event` and the detail fields, as today. `prompt_deleted` is `true` only when retention removed the prompt, and `prompt` is then null.

- `day` is the UTC day of the record time. It is required, in the form `YYYY-MM-DD` only. 400 `invalid_time` if it is missing or has another form.
- 404 `record_not_found` if the day has no record with this id.
- 502 `prompt_unreadable` if the prompt file exists and the read fails, or if its line does not hold the record. A file of the old layout that is missing gives the same error.
- 403 `admin_required` for a caller who is not an admin. The acting user rule applies.

### `GET /admin/audit/users`, usage endpoints

No change in the answers. The days `since` and `until` of the usage endpoints accept only the form `YYYY-MM-DD`. Any other form gets 400 `invalid_time`.

### Admin app

- New backend route `GET /api/audit/records/{request_id}?day=` that calls the new endpoint.
- The audit table shows the event tag from `event`.
- A click on a row loads the full record with the new route, then shows it as today. If the prompt is deleted, the dialog says so.

## Limits

- One service instance writes the log. Two instances on one blob root are not supported. This is true today.
- A day file with N rows is read in one piece. The design fits about 300,000 requests a day. Above that, the day file gets large and needs a split.
- A crash loses the records that wait in the buffer, at most 5 seconds of them. This is true today.
- A prompt is not searchable. The filters work on metadata.
- A blob write can fail after the blob was written. The records then stay in the buffer. The next flush writes them again under a new segment name, so they appear two times.
- The merges assume that a blob write is atomic. A reader never sees half a file.
- A clock that steps back across a restart can give a new segment a name that is not greater than the last checkpoint. A crash before the next checkpoint then loses those rows from the usage totals.
- A stop of the service waits for a merge that runs at that moment.
- A late record for a day whose daily usage file exists counts in memory at once. The service writes it to the daily file at the next flusher cycle. A crash between the two loses it from the usage totals.

## Testing

- Flush: prompt file before segment file, the two failure cases, and the write-through cache.
- Merges: parts and day files, a late record in a finished day, a stop between the write and the deletes, and no merge of a bucket that is not ready.
- Reads: the `seg` rule with duplicated inputs, the old layout, the order of records, and a list that makes no prompt read.
- Usage: the checkpoint holds only flushed rows, a start from a checkpoint reads only later rows, and the three start cases.
- Retention: deletion after N days, the answer for a deleted prompt, and no deletion when the variable is not set.
- API: the new endpoint with its error cases, and the list without prompts.
- Admin app: the new route and the record dialog.
- On an installation: a load run that writes a few thousand records, then a restart, with the start time and the audit page time measured.

## Out of scope

- A migration that rewrites old layout files.
- A retention time for metadata.
- A search in prompts.
- More than one service instance.
