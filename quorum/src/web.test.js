const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

const context = {globalThis: {}, TextDecoder};
vm.runInNewContext(fs.readFileSync('quorum/src/web.js', 'utf8'), context);
const {MAX_NORMALIZED_EVENTS_PER_RECORD, MAX_RENDERED_TAIL_ROWS, MAX_RENDERED_ROWS_PER_POLL, MAX_NORMALIZED_RECORDS_PER_POLL, MAX_PENDING_STREAM_BYTES, MAX_COCKPIT_ITEMS, stripShellWrapper, commandSummary, normalizeEvent, normalizeEvents, parseEventLine, normalizeTail, reassembleTail, detailNavigationState, liveAgentTitle, shouldTrim, bounded, navigableRuns, textValue, pollState, dashboardModel} = context.globalThis.QuorumWeb;

assert.equal(stripShellWrapper('/bin/zsh -lc "git status"'), 'git status');
assert.equal(stripShellWrapper("/bin/zsh -lc 'git status'"), 'git status');
assert.equal(commandSummary('git status'), 'git status');
assert.equal(stripShellWrapper('/bin/bash -lc "echo \\"quoted\\""'), 'echo "quoted"');

const codexCommand = normalizeEvent({type: 'item.completed', item: {type: 'command_execution', command: '/bin/zsh -lc "pwd"', aggregated_output: '/tmp', exit_code: 0}});
assert.equal(codexCommand.kind, 'command');
assert.equal(codexCommand.title, 'pwd');
assert.equal(codexCommand.exit_code, 0);
assert.equal(normalizeEvent({type: 'item.completed', item: {type: 'agent_message', text: 'hello'}}).kind, 'message');
assert.equal(normalizeEvent({type: 'assistant', message: {content: [{type: 'tool_use', name: 'Bash', input: {command: 'pwd'}}]}}).kind, 'command');
const claudeString = normalizeEvent({type: 'assistant', message: {content: 'Claude prose'}});
assert.equal(claudeString.kind, 'message');
assert.equal(claudeString.body, 'Claude prose');
assert.equal(normalizeEvent({type: 'future.event', payload: {}}).kind, 'unknown');
assert.equal(parseEventLine('{"type":"item.completed"')[0].kind, 'unknown');

const claudeTool = normalizeEvents({type: 'tool_use', name: 'Bash', input: {command: 'cargo test'}});
assert.equal(claudeTool.length, 1);
assert.equal(claudeTool[0].kind, 'command');
assert.match(claudeTool[0].title, /Bash/);

const claudeResult = normalizeEvents({type: 'result', result: 'done', usage: {input_tokens: 10, output_tokens: 5}});
assert.equal(claudeResult.length, 2);
assert.equal(claudeResult[0].body, 'done');
assert.equal(claudeResult[1].kind, 'meta');
assert.match(claudeResult[1].title, /15 tokens/);

const mixedAssistant = normalizeEvents({type: 'assistant', message: {content: [
  {type: 'text', text: 'Checking'},
  {type: 'tool_use', name: 'Bash', input: {command: 'cargo test'}},
  {type: 'tool_use', name: 'Read', input: {file_path: '/tmp/log'}},
]}});
assert.equal(mixedAssistant.map(row => row.kind).join(','), 'message,command,command');
assert.match(mixedAssistant[2].title, /Read/);

const denseAssistant = normalizeEvents({type: 'assistant', message: {content: Array.from({length: 1_000}, () => ({}))}});
assert.equal(denseAssistant.length, MAX_NORMALIZED_EVENTS_PER_RECORD);
assert.equal(denseAssistant.at(-1).kind, 'meta');
assert.match(denseAssistant.at(-1).title, /901 assistant blocks omitted/);

// `item.started` is superseded by its `item.completed`; rendering both duplicates every row.
assert.equal(normalizeEvents({type: 'item.started', item: {id: 'item_0', type: 'command_execution'}}).length, 0);

const fileChange = normalizeEvent({type: 'item.completed', item: {type: 'file_change', changes: [{kind: 'modify', path: 'src/web.rs'}]}});
assert.equal(fileChange.kind, 'meta');
assert.match(fileChange.title, /modify src\/web\.rs/);

// Empty-bodied rows cost zero characters, so the tail bound must count rows as well —
// otherwise a stream of `{"message":{"content":""}}` grows the DOM without limit.
const emptyMessage = normalizeEvent({type: 'assistant', message: {content: ''}});
assert.equal(emptyMessage.body, '');
assert.equal(shouldTrim(0, MAX_RENDERED_TAIL_ROWS + 1), true);
assert.equal(shouldTrim(0, MAX_RENDERED_TAIL_ROWS), false);

const repeatedEmpties = Array.from({length: 50_000}, () => '{"type":"assistant","message":{"content":""}}');
let normalizedRecords = 0;
const capped = normalizeTail(repeatedEmpties, MAX_RENDERED_ROWS_PER_POLL, MAX_NORMALIZED_RECORDS_PER_POLL, line => {
  normalizedRecords++;
  return parseEventLine(line);
});
assert.equal(normalizedRecords, MAX_NORMALIZED_RECORDS_PER_POLL);
assert.equal(capped.length, MAX_RENDERED_ROWS_PER_POLL);
assert.equal(capped[0].kind, 'meta');
assert.match(capped[0].title, new RegExp(`${50_000 - (MAX_RENDERED_ROWS_PER_POLL - 1)} earlier events omitted`));

const denseUnknowns = Array.from({length: 50_000}, () => '{"type":"future.event"}');
let parsedUnknowns = 0;
const denseCapped = normalizeTail(denseUnknowns, MAX_RENDERED_ROWS_PER_POLL, MAX_NORMALIZED_RECORDS_PER_POLL, line => {
  parsedUnknowns++;
  return parseEventLine(line);
});
assert.equal(parsedUnknowns, MAX_NORMALIZED_RECORDS_PER_POLL);
assert.equal(denseCapped.length, MAX_RENDERED_ROWS_PER_POLL);
assert.match(denseCapped[0].title, new RegExp(`${50_000 - (MAX_RENDERED_ROWS_PER_POLL - 1)} earlier events omitted`));

let tailState = {};
let tail = reassembleTail(tailState, [], '7b2274797065223a22617373697374616e74222c226d657373616765223a7b22636f6e74656e74223a2270617274', false);
tailState = tail.state;
assert.equal(tail.lines.length, 0);
tail = reassembleTail(tailState, ['69616c227d7d'], null, false);
assert.equal(tail.lines[0], '{"type":"assistant","message":{"content":"partial"}}');
assert.equal(parseEventLine(tail.lines[0])[0].body, 'partial');

tail = reassembleTail({}, ['7472756e6361746564', '7b2274797065223a22617373697374616e74222c226d657373616765223a7b22636f6e74656e74223a2277686f6c65227d7d'], null, true);
assert.equal(tail.lines.length, 1);
assert.equal(parseEventLine(tail.lines[0])[0].body, 'whole');

tail = reassembleTail({}, [], '61'.repeat(MAX_PENDING_STREAM_BYTES + 1), false);
assert.equal(tail.state.pending.length, 0);
assert.equal(tail.state.discardLeading, true);

// A dense 2 MiB response can contain hundreds of thousands of complete records.
// Reassembly must retain/decode only the suffix that normalization can inspect.
const denseLines = Array.from({length: 50_000}, () => '7b7d');
tail = reassembleTail({}, denseLines, null, false);
assert.equal(tail.lines.length, MAX_NORMALIZED_RECORDS_PER_POLL);
assert.equal(tail.omitted, denseLines.length - MAX_NORMALIZED_RECORDS_PER_POLL);
assert.equal(tail.lines[0], '{}');

// The endpoint may already have dropped a dense prefix; its omission count must reach
// the normalizer without expanding the retained transport suffix.
tail = reassembleTail({}, denseLines.slice(-MAX_NORMALIZED_RECORDS_PER_POLL), null, false, 48_000);
assert.equal(tail.lines.length, MAX_NORMALIZED_RECORDS_PER_POLL);
assert.equal(tail.omitted, 48_000);

// Explicit navigation always starts a new, live view; offset zero is a replacement,
// not an append to a paused stream or its partial continuation.
const detail = detailNavigationState('B-200', 0);
assert.equal(detail.openRun, 'B-200');
assert.equal(detail.offset, 0);
assert.equal(detail.paused, false);
assert.equal(detail.rawMode, false);
assert.equal(detail.renderedChars, 0);
assert.equal(Object.keys(detail.tailState).length, 0);

assert.equal(liveAgentTitle({name: 'A-100', task_id: 42}), 'A-100 · Task #42 · Live tail');
assert.equal(liveAgentTitle({name: 'A-100', task_id: null}), 'A-100 · Live tail');
assert.match(fs.readFileSync('quorum/src/web.html', 'utf8'), /id="liveAgentTable"/);

// The initial dashboard promotes only task health, active work, attention, pipeline, and
// the server's ready/claimable projection. A stale recent-task list must not replace queue_bands.ready.
const model = dashboardModel({
  health: {verdict: 'attention'},
  working_now: Array.from({length: MAX_COCKPIT_ITEMS + 3}, (_, id) => ({name: `agent-${id}`})),
  tasks: [{id: 999, title: 'not the queue'}],
  queue_bands: {
    planning: {count: 2}, ready: {count: 4, tasks: [{id: 7, title: 'authoritative'}]},
    working: {count: 3}, reviewing: {count: 1}, merging: {count: 1},
    terminal: {failed: 2, cancelled: 1}, attention: {count: 2},
  },
  needs_attention: {blocked_tasks: {count: 1, tasks: [{id: 8}]}, alerts: [{body: 'watch'}]},
});
assert.equal(model.queue.length, 1);
assert.equal(model.queue[0].id, 7);
assert.equal(model.working.length, MAX_COCKPIT_ITEMS);
assert.equal(model.pipeline.at(-1)[0], 'Terminal outcomes');
assert.equal(model.pipeline.at(-1)[1], 3);
assert.equal(model.attention[0][0], 'Blocked tasks');

// Stored values remain text data. The browser renderer assigns this through textContent;
// there is no HTML interpolation path for task titles, alerts, or agent names.
assert.equal(textValue('<img src=x onerror=alert(1)>'), '<img src=x onerror=alert(1)>');
assert.equal(textValue(null), '');
assert.equal(bounded(Array.from({length: MAX_COCKPIT_ITEMS + 1}), MAX_COCKPIT_ITEMS).length, MAX_COCKPIT_ITEMS);
const client = fs.readFileSync('quorum/src/web.js', 'utf8');
assert.doesNotMatch(client, /\.innerHTML\s*=/);
assert.match(client, /textContent/);

// Task-detail runs are database records and deliberately do not contain session-log `dir`.
// They must not turn into a nonfunctional `#run-undefined` stream link.
assert.equal(navigableRuns([{id: 12, agent: 'A-12', role: 'worker'}]).length, 0);
assert.equal(navigableRuns([{dir: 'A-12-42', meta: {agent: 'A-12'}}]).length, 1);

// A hidden/failed refresh cannot make a second request overlap; failure visibly marks the
// last successful data stale and the following success clears that condition.
let refresh = pollState({inFlight: false, lastSuccess: null, failed: false, stale: false}, 'start', 100);
assert.equal(refresh.inFlight, true);
refresh = pollState({inFlight: false, lastSuccess: 100, failed: false, stale: false}, 'failure', 105);
assert.equal(refresh.stale, true);
assert.equal(refresh.failed, true);
refresh = pollState(refresh, 'success', 106);
assert.equal(refresh.inFlight, false);
assert.equal(refresh.lastSuccess, 106);
assert.equal(refresh.failed, false);
assert.equal(refresh.stale, false);
