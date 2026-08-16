(() => {
  const MAX_RENDERED_TAIL_CHARS = 2 * 1024 * 1024;
  const MAX_RENDERED_TAIL_ROWS = 2000;
  const MAX_RENDERED_ROWS_PER_POLL = 2000;
  const MAX_NORMALIZED_RECORDS_PER_POLL = 2000;
  const MAX_PENDING_STREAM_BYTES = 8 * 1024 * 1024;
  const MAX_EXPANDED_OUTPUT_CHARS = 200 * 1024;
  const MAX_NORMALIZED_EVENTS_PER_RECORD = 100;
  const ellipsize = (text, max = 90) => text.length > max ? text.slice(0, max - 1) + '…' : text;

  // Row count, not character count, is the load-bearing bound: a producer shape whose rows
  // render as empty text (`{"message":{"content":""}}`) costs zero chars and would otherwise
  // accumulate forever. Bounding at the DOM-append boundary covers every shape, present and future.
  const shouldTrim = (chars, rows) => chars > MAX_RENDERED_TAIL_CHARS || rows > MAX_RENDERED_TAIL_ROWS;

  // Walk newest-to-oldest so a dense bounded response shows its useful tail without first
  // allocating a row for every JSONL record. One row is reserved for the omission marker.
  function normalizeTail(lines, maxRows = MAX_RENDERED_ROWS_PER_POLL, maxRecords = MAX_NORMALIZED_RECORDS_PER_POLL, parseLine = parseEventLine, omittedBefore = 0) {
    const groups = [];
    let retainedRows = 0;
    let index = lines.length - 1;
    for (; index >= 0 && lines.length - 1 - index < maxRecords; index--) {
      const rows = parseLine(lines[index]);
      if (retainedRows + rows.length > maxRows - 1) break;
      if (rows.length) {
        groups.push(rows);
        retainedRows += rows.length;
      }
    }
    const omitted = omittedBefore + index + 1;
    const retained = groups.reverse().flat();
    return omitted
      ? [{kind: 'meta', title: `${omitted} earlier events omitted`, body: '', exit_code: null}, ...retained]
      : retained;
  }

  function stripShellWrapper(command) {
    const match = String(command || '').match(/^(?:\/bin\/(?:zsh|bash) -lc|sh -c) (["'])([\s\S]*)\1$/);
    if (!match) return String(command || '');
    return match[2].replace(/\\"/g, '"').replace(/\\\\/g, '\\');
  }

  function commandSummary(command) {
    return ellipsize(stripShellWrapper(command).replace(/\s+/g, ' ').trim());
  }

  function commandEvent(name, input) {
    return {kind: 'command', title: `${name || 'tool'} ${ellipsize(JSON.stringify(input || {}), 90)}`, body: {command: name || 'tool', output: ''}, exit_code: null};
  }

  function usageEvent(usage, title = 'Turn completed') {
    const input = Number(usage && usage.input_tokens || 0), cached = Number(usage && usage.cached_input_tokens || 0), output = Number(usage && usage.output_tokens || 0), reasoning = Number(usage && usage.reasoning_output_tokens || 0);
    return {kind: 'meta', title: `${title} · ${input + output + reasoning} tokens${input ? ` · ${Math.round(cached * 100 / input)}% cached` : ''}`, body: '', exit_code: null};
  }

  function normalizeEvents(event) {
    const item = event && event.item;
    // The matching `item.completed` supersedes it; rendering both duplicates every row.
    if (event && event.type === 'item.started') return [];
    if (event && event.type === 'item.completed' && item && item.type === 'file_change') {
      const changes = Array.isArray(item.changes) ? item.changes : [];
      const summary = changes.slice(0, 5).map(change => `${change.kind || 'change'} ${change.path || '?'}`).join(', ');
      return [{kind: 'meta', title: `file change · ${ellipsize(summary || 'no paths', 90)}${changes.length > 5 ? ` (+${changes.length - 5} more)` : ''}`, body: '', exit_code: null}];
    }
    if (event && event.type === 'item.completed' && item && item.type === 'agent_message') {
      return [{kind: 'message', title: 'Agent message', body: String(item.text || ''), exit_code: null}];
    }
    if (event && event.type === 'item.completed' && item && item.type === 'command_execution') {
      return [{kind: 'command', title: commandSummary(item.command), body: {command: String(item.command || ''), output: String(item.aggregated_output || '')}, exit_code: item.exit_code ?? null}];
    }
    if (event && event.type === 'turn.completed') {
      return [usageEvent(event.usage)];
    }
    if (event && (event.type === 'provider.stderr' || event.type === 'thread.started')) {
      return [{kind: 'meta', title: event.type, body: String(event.text || event.thread_id || ''), exit_code: null}];
    }
    if (event && event.type === 'assistant') {
      const content = event.message && event.message.content;
      if (typeof content === 'string') {
        return [{kind: 'message', title: 'Assistant message', body: content, exit_code: null}];
      }
      const blocks = Array.isArray(content) ? content : [];
      if (blocks.length) {
        const visible = blocks.slice(0, MAX_NORMALIZED_EVENTS_PER_RECORD);
        const rows = visible.map(part => {
        if (part.type === 'text') return {kind: 'message', title: 'Assistant message', body: String(part.text || ''), exit_code: null};
        if (part.type === 'tool_use') return commandEvent(part.name, part.input);
        return {kind: 'unknown', title: `Unrecognized assistant block: ${part.type || 'unknown'}`, body: '', exit_code: null};
        });
        if (blocks.length > MAX_NORMALIZED_EVENTS_PER_RECORD) {
          rows[MAX_NORMALIZED_EVENTS_PER_RECORD - 1] = {kind: 'meta', title: `${blocks.length - MAX_NORMALIZED_EVENTS_PER_RECORD + 1} assistant blocks omitted`, body: '', exit_code: null};
        }
        return rows;
      }
    }
    if (event && event.type === 'tool_use') {
      return [commandEvent(event.name, event.input)];
    }
    if (event && event.type === 'result') {
      const result = typeof event.result === 'string' ? event.result : JSON.stringify(event.result || '');
      const rows = [{kind: 'message', title: event.is_error ? 'Result error' : 'Result', body: result, exit_code: null}];
      if (event.usage) rows.push(usageEvent(event.usage, 'Result completed'));
      return rows;
    }
    if (event && event.type === 'user') {
      const content = event.message && event.message.content;
      const result = Array.isArray(content) ? content.find(part => part.type === 'tool_result') : content && content.type === 'tool_result' ? content : null;
      if (result) return [{kind: 'result', title: 'Tool result', body: typeof result.content === 'string' ? result.content : JSON.stringify(result.content || ''), exit_code: null}];
    }
    return [{kind: 'unknown', title: event && event.type ? `Unrecognized: ${event.type}` : 'Unrecognized event', body: '', exit_code: null}];
  }

  function normalizeEvent(event) {
    return normalizeEvents(event)[0];
  }

  function parseEventLine(line) {
    try { return normalizeEvents(JSON.parse(line)); }
    catch (_) { return normalizeEvents(null); }
  }

  const utf8 = new TextDecoder('utf-8', {fatal: true});
  function hexBytes(hex) { const bytes = new Uint8Array(hex.length / 2); for (let i = 0; i < bytes.length; i++) bytes[i] = parseInt(hex.slice(i * 2, i * 2 + 2), 16); return bytes; }
  function joinBytes(left, right) { const joined = new Uint8Array(left.length + right.length); joined.set(left); joined.set(right, left.length); return joined; }
  // Keep only the suffix that can reach the bounded normalizer. `lines` itself comes
  // from a 2 MiB JSON response and can contain hundreds of thousands of tiny records.
  function reassembleTail(state, lines, partial, startsMidLine = false, omittedBefore = 0, maxRecords = MAX_NORMALIZED_RECORDS_PER_POLL) {
    let {pending = new Uint8Array(), discardLeading = startsMidLine} = state;
    const complete = [];
    const first = discardLeading ? 1 : 0;
    const start = Math.max(first, lines.length - maxRecords);
    const omitted = omittedBefore + start - first;
    // A pending record can only join `lines[0]`. If that record is outside the
    // retained suffix, discard it rather than decoding an otherwise invisible prefix.
    if (start > 0) pending = new Uint8Array();
    for (let index = start; index < lines.length; index++) {
      const bytes = joinBytes(pending, hexBytes(lines[index]));
      try { complete.push(utf8.decode(bytes)); }
      catch (_) { complete.push(''); }
      pending = new Uint8Array();
    }
    if (lines.length) discardLeading = false;
    if (partial != null && !discardLeading) pending = joinBytes(pending, hexBytes(partial));
    const truncated = pending.length > MAX_PENDING_STREAM_BYTES;
    if (truncated) { pending = new Uint8Array(); discardLeading = true; }
    return {lines: complete, omitted, truncated, state: {pending, discardLeading}};
  }

  function detailNavigationState(dir, from) {
    return {openRun: dir, offset: from ?? null, paused: false, rawMode: false, rawText: '', renderedChars: 0, tailState: {}};
  }
  function liveAgentTitle(agent) { return `${agent.name}${agent.task_id ? ` · Task #${agent.task_id}` : ''} · Live tail`; }

  globalThis.QuorumWeb = {MAX_NORMALIZED_EVENTS_PER_RECORD, MAX_RENDERED_TAIL_ROWS, MAX_RENDERED_ROWS_PER_POLL, MAX_NORMALIZED_RECORDS_PER_POLL, MAX_PENDING_STREAM_BYTES, stripShellWrapper, commandSummary, normalizeEvent, normalizeEvents, parseEventLine, normalizeTail, reassembleTail, detailNavigationState, liveAgentTitle, shouldTrim};
  if (typeof document === 'undefined') return;

  let openRun = null, openRunTitle = null, offset = null, paused = false, rawMode = false, rawText = '', renderedChars = 0, runsBefore = null, tailState = {}, tailInFlight = false, tailEpoch = 0, runsEpoch = 0, currentRunsBefore = null;
  const $ = id => document.getElementById(id);
  const put = (node, value) => { node.textContent = value ?? ''; return node; };
  const node = value => put(document.createTextNode(''), value);
  function show(id) { document.querySelectorAll('main').forEach(n => n.classList.toggle('hidden', n.id !== id)); }
  document.querySelectorAll('[data-view]').forEach(button => button.addEventListener('click', () => show(button.dataset.view)));
  function appendRow(table, values, head = false) { const tr = document.createElement('tr'); values.forEach(value => { const cell = document.createElement(head ? 'th' : 'td'); cell.append(value instanceof Node ? value : node(value)); tr.append(cell); }); table.append(tr); return tr; }
  function age(seconds) { return seconds < 60 ? seconds + 's' : Math.floor(seconds / 60) + 'm'; }
  function relativeTime(epoch, now) { const seconds = Math.max(0, now - epoch); return seconds < 60 ? `${seconds}s ago` : seconds < 3600 ? `${Math.floor(seconds / 60)}m ago` : seconds < 86400 ? `${Math.floor(seconds / 3600)}h ago` : `${Math.floor(seconds / 86400)}d ago`; }
  function timestamp(epoch) { return new Date(epoch * 1000).toLocaleString(); }
  function trimStream() { const stream = $('stream'); while (shouldTrim(renderedChars, stream.childElementCount) && stream.firstChild) { renderedChars -= stream.firstChild.textContent.length; stream.firstChild.remove(); } }
  function exitBadge(code) { const badge = document.createElement('span'); badge.className = `badge ${code === 0 ? 'ok' : code == null ? '' : 'bad'}`; put(badge, code == null ? 'in progress' : `exit ${code}`); return badge; }
  function capOutput(output) { return output.length > MAX_EXPANDED_OUTPUT_CHARS ? output.slice(0, MAX_EXPANDED_OUTPUT_CHARS) + '\n… output truncated …' : output; }
  function renderEvent(event) {
    if (event && event.type === 'item.started') return;
    const entry = event && event.kind ? event : normalizeEvent(event), row = document.createElement('div'); row.className = `run-row run-${entry.kind}`;
    if (entry.kind === 'message') { put(row, entry.body); }
    else if (entry.kind === 'command') { const details = document.createElement('details'), summary = document.createElement('summary'), pre = document.createElement('pre'); details.className = 'run-command'; put(summary, `$ ${entry.title}`); summary.append(exitBadge(entry.exit_code)); put(pre, `${entry.body.command}\n\n${capOutput(entry.body.output)}`); details.append(summary, pre); row.append(details); }
    else { const title = document.createElement('span'); title.className = 'dim'; put(title, entry.title + (entry.body ? ` · ${entry.body}` : '')); row.append(title); }
    renderedChars += row.textContent.length; $('stream').append(row); trimStream();
  }
  function renderRaw() { $('rawStream').textContent = rawText.length > MAX_RENDERED_TAIL_CHARS ? rawText.slice(-MAX_RENDERED_TAIL_CHARS) : rawText; }
  function appendTail(lines, partial, startsMidLine, omitted, replace) { if (replace) { $('stream').replaceChildren(); renderedChars = 0; rawText = ''; tailState = {}; } const reassembled = reassembleTail(tailState, lines, partial, startsMidLine, omitted); tailState = reassembled.state; const text = reassembled.lines.join('\n') + (reassembled.lines.length ? '\n' : ''); const truncation = reassembled.truncated ? '[oversized stream record omitted]\n' : ''; rawText = (rawText + text + truncation).slice(-MAX_RENDERED_TAIL_CHARS); const rendered = normalizeTail(reassembled.lines, MAX_RENDERED_ROWS_PER_POLL, MAX_NORMALIZED_RECORDS_PER_POLL, parseEventLine, reassembled.omitted); if (reassembled.truncated) rendered.unshift({kind: 'meta', title: 'oversized stream record omitted', body: '', exit_code: null}); rendered.forEach(renderEvent); if (rawMode) renderRaw(); }
  async function state() { if (document.hidden) return; const s = await fetch('/api/state').then(r => r.json()), counts = Object.fromEntries(s.counts.map(x => [x.status, x.count])); $('tiles').replaceChildren(); ['working', 'open', 'in-review', 'done'].forEach(key => { const tile = document.createElement('span'); tile.className = 'tile'; put(tile, key + '\n' + (counts[key] || 0)); $('tiles').append(tile); }); const tasks = $('tasks'); tasks.replaceChildren(); appendRow(tasks, ['State', 'Task', 'Provider/model', 'PR', 'Age'], true); s.tasks.forEach(x => { const pr = document.createElement('span'); if (x.pr) { const link = document.createElement('a'); link.href = 'https://github.com/ag2trust/quorum/pull/' + x.pr; put(link, '#' + x.pr); pr.append(link); } appendRow(tasks, [x.state, '#' + x.id + ' ' + x.title, (x.provider || 'pending') + ' ' + (x.model || ''), pr, age(x.age_secs)]); }); renderLiveAgents(s.live_agents); renderAgents(s.agents, s.now); $('alerts').textContent = JSON.stringify({alerts: s.alerts, errors: s.errors}, null, 2); }
  function renderLiveAgents(agents) { const table = $('liveAgentTable'); table.replaceChildren(); appendRow(table, ['Agent', 'Role / phase', 'Task', 'Provider / model', 'Now', 'Activity'], true); agents.forEach(agent => { const name = document.createElement(agent.run_dir ? 'button' : 'span'); put(name, agent.name); if (agent.run_dir) name.addEventListener('click', () => openDetail(agent.run_dir, null, liveAgentTitle(agent))); appendRow(table, [name, `${agent.role || '—'} / ${agent.phase || '—'}`, agent.task_id == null ? '—' : `#${agent.task_id} ${agent.task_title || ''}`, `${agent.provider || ''} ${agent.model || ''}`.trim() || '—', agent.now || '—', agent.last_activity_age_secs == null ? '—' : `${age(agent.last_activity_age_secs)} ago`]); }); }
  function renderAgents(agents, now) { const online = agents.filter(agent => agent.online), offline = agents.filter(agent => !agent.online); const render = (table, rows) => { table.replaceChildren(); appendRow(table, ['Agent', 'Task', 'Last seen'], true); rows.forEach(agent => { const seen = document.createElement('span'); put(seen, relativeTime(agent.last_seen, now)); seen.title = timestamp(agent.last_seen); appendRow(table, [agent.name, agent.task_held ? '#' + agent.task_held.id + ' ' + agent.task_held.title : '—', seen]); }); }; render($('agentTable'), online); render($('offlineAgentTable'), offline); $('offlineAgents').classList.toggle('hidden', !offline.length); put($('offlineAgents').querySelector('summary'), `${offline.length} offline`); }
  function duration(meta) { const start = meta.start_time, end = meta.end_time; return start && end ? age(Math.max(0, end - start)) : '—'; }
  function runTokens(meta) { return meta.cost_tokens == null ? '—' : Number(meta.cost_tokens).toLocaleString(); }
  function renderRuns(items, replace) { const table = $('runs'); if (replace) { table.replaceChildren(); appendRow(table, ['Agent', 'Role', 'Task', 'Duration', 'Final phase', 'Verdict', 'Tokens'], true); } items.forEach(run => { const meta = run.meta || {}, tr = appendRow(table, [meta.agent || '—', meta.role || '—', meta.task_id == null ? '—' : '#' + meta.task_id, duration(meta), meta.final_phase || '—', meta.verdict || '—', runTokens(meta)]); tr.className = 'clickable'; tr.dataset.runDir = run.dir; tr.addEventListener('click', () => openDetail(run.dir)); }); }
  async function runs(before = currentRunsBefore, explicit = false) { if (document.hidden || (!explicit && currentRunsBefore !== null)) return; if (explicit) currentRunsBefore = before; const epoch = ++runsEpoch, suffix = before ? '?before=' + encodeURIComponent(before) : ''; const response = await fetch('/api/runs' + suffix).then(x => x.json()); if (epoch !== runsEpoch) return; renderRuns(response.runs, true); currentRunsBefore = before; runsBefore = response.next_before; $('moreRuns').classList.toggle('hidden', !runsBefore); $('newestRuns').classList.toggle('hidden', !currentRunsBefore); }
  async function openDetail(dir, from, title = dir) { tailEpoch++; const next = detailNavigationState(dir, from); ({openRun, offset, paused, rawMode, rawText, renderedChars, tailState} = next); openRunTitle = title; $('stream').replaceChildren(); put($('pause'), 'Pause tail'); put($('rawToggle'), 'Show raw'); $('rawStream').classList.add('hidden'); $('stream').classList.remove('hidden'); show('run'); put($('runTitle'), title); await tail(); }
  async function tail() { if (!openRun || paused || document.hidden || tailInFlight) return; tailInFlight = true; const epoch = tailEpoch, run = openRun, from = offset; try { const url = '/api/runs/' + encodeURIComponent(run) + '/stream?max=2097152' + (from !== null ? '&from=' + from : ''); const response = await fetch(url).then(x => x.json()); if (epoch !== tailEpoch || run !== openRun) return; appendTail(response.lines, response.partial, response.starts_mid_line, response.omitted || 0, from === null); offset = response.next_offset; $('stream').scrollTop = $('stream').scrollHeight; } finally { tailInFlight = false; if (epoch !== tailEpoch) tail(); } }
  $('pause').onclick = () => { paused = !paused; put($('pause'), paused ? 'Resume tail' : 'Pause tail'); };
  $('start').onclick = event => { event.preventDefault(); openDetail(openRun, 0, openRunTitle); };
  $('rawToggle').onclick = () => { rawMode = !rawMode; $('stream').classList.toggle('hidden', rawMode); $('rawStream').classList.toggle('hidden', !rawMode); put($('rawToggle'), rawMode ? 'Show rendered' : 'Show raw'); if (rawMode) renderRaw(); };
  $('moreRuns').onclick = () => runs(runsBefore, true);
  $('newestRuns').onclick = () => runs(null, true);
  document.addEventListener('visibilitychange', () => { if (!document.hidden) { state(); runs(); tail(); } });
  state(); runs(); setInterval(state, 2000); setInterval(() => runs(), 5000); setInterval(tail, 1000);
})();
