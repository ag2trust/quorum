(() => {
  const MAX_RENDERED_TAIL_CHARS = 2 * 1024 * 1024;
  const MAX_RENDERED_TAIL_ROWS = 2000;
  const MAX_RENDERED_ROWS_PER_POLL = 2000;
  const MAX_NORMALIZED_RECORDS_PER_POLL = 2000;
  const MAX_EXPANDED_OUTPUT_CHARS = 200 * 1024;
  const MAX_NORMALIZED_EVENTS_PER_RECORD = 100;
  const ellipsize = (text, max = 90) => text.length > max ? text.slice(0, max - 1) + '…' : text;

  // Row count, not character count, is the load-bearing bound: a producer shape whose rows
  // render as empty text (`{"message":{"content":""}}`) costs zero chars and would otherwise
  // accumulate forever. Bounding at the DOM-append boundary covers every shape, present and future.
  const shouldTrim = (chars, rows) => chars > MAX_RENDERED_TAIL_CHARS || rows > MAX_RENDERED_TAIL_ROWS;

  // Walk newest-to-oldest so a dense bounded response shows its useful tail without first
  // allocating a row for every JSONL record. One row is reserved for the omission marker.
  function normalizeTail(lines, maxRows = MAX_RENDERED_ROWS_PER_POLL, maxRecords = MAX_NORMALIZED_RECORDS_PER_POLL, parseLine = parseEventLine) {
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
    const omitted = index + 1;
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

  globalThis.QuorumWeb = {MAX_NORMALIZED_EVENTS_PER_RECORD, MAX_RENDERED_TAIL_ROWS, MAX_RENDERED_ROWS_PER_POLL, MAX_NORMALIZED_RECORDS_PER_POLL, stripShellWrapper, commandSummary, normalizeEvent, normalizeEvents, parseEventLine, normalizeTail, shouldTrim};
  if (typeof document === 'undefined') return;

  let openRun = null, offset = null, paused = false, rawMode = false, rawText = '', renderedChars = 0, runsBefore = null;
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
  function appendTail(lines, replace) { if (replace) { $('stream').replaceChildren(); renderedChars = 0; rawText = ''; } const text = lines.join('\n') + (lines.length ? '\n' : ''); rawText = (rawText + text).slice(-MAX_RENDERED_TAIL_CHARS); normalizeTail(lines).forEach(renderEvent); if (rawMode) renderRaw(); }
  async function state() { if (document.hidden) return; const s = await fetch('/api/state').then(r => r.json()), counts = Object.fromEntries(s.counts.map(x => [x.status, x.count])); $('tiles').replaceChildren(); ['working', 'open', 'in-review', 'done'].forEach(key => { const tile = document.createElement('span'); tile.className = 'tile'; put(tile, key + '\n' + (counts[key] || 0)); $('tiles').append(tile); }); const tasks = $('tasks'); tasks.replaceChildren(); appendRow(tasks, ['State', 'Task', 'Provider/model', 'PR', 'Age'], true); s.tasks.forEach(x => { const pr = document.createElement('span'); if (x.pr) { const link = document.createElement('a'); link.href = 'https://github.com/ag2trust/quorum/pull/' + x.pr; put(link, '#' + x.pr); pr.append(link); } appendRow(tasks, [x.state, '#' + x.id + ' ' + x.title, (x.provider || 'pending') + ' ' + (x.model || ''), pr, age(x.age_secs)]); }); renderAgents(s.agents, s.now); $('alerts').textContent = JSON.stringify({alerts: s.alerts, errors: s.errors}, null, 2); }
  function renderAgents(agents, now) { const online = agents.filter(agent => agent.online), offline = agents.filter(agent => !agent.online); const render = (table, rows) => { table.replaceChildren(); appendRow(table, ['Agent', 'Task', 'Last seen'], true); rows.forEach(agent => { const seen = document.createElement('span'); put(seen, relativeTime(agent.last_seen, now)); seen.title = timestamp(agent.last_seen); appendRow(table, [agent.name, agent.task_held ? '#' + agent.task_held.id + ' ' + agent.task_held.title : '—', seen]); }); }; render($('agentTable'), online); render($('offlineAgentTable'), offline); $('offlineAgents').classList.toggle('hidden', !offline.length); put($('offlineAgents').querySelector('summary'), `${offline.length} offline`); }
  function duration(meta) { const start = meta.start_time, end = meta.end_time; return start && end ? age(Math.max(0, end - start)) : '—'; }
  function runTokens(meta) { return meta.cost_tokens == null ? '—' : Number(meta.cost_tokens).toLocaleString(); }
  function renderRuns(items, replace) { const table = $('runs'); if (replace) { table.replaceChildren(); appendRow(table, ['Agent', 'Role', 'Task', 'Duration', 'Final phase', 'Verdict', 'Tokens'], true); } items.forEach(run => { const meta = run.meta || {}, tr = appendRow(table, [meta.agent || '—', meta.role || '—', meta.task_id == null ? '—' : '#' + meta.task_id, duration(meta), meta.final_phase || '—', meta.verdict || '—', runTokens(meta)]); tr.className = 'clickable'; tr.dataset.runDir = run.dir; tr.addEventListener('click', () => openDetail(run.dir)); }); }
  let currentRunsBefore = null;
  async function runs(before = currentRunsBefore) { if (document.hidden) return; const suffix = before ? '?before=' + encodeURIComponent(before) : ''; const response = await fetch('/api/runs' + suffix).then(x => x.json()); renderRuns(response.runs, true); currentRunsBefore = before; runsBefore = response.next_before; $('moreRuns').classList.toggle('hidden', !runsBefore); }
  async function openDetail(dir, from) { openRun = dir; offset = from ?? null; rawText = ''; rawMode = false; put($('rawToggle'), 'Show raw'); $('rawStream').classList.add('hidden'); $('stream').classList.remove('hidden'); show('run'); put($('runTitle'), dir); await tail(); }
  async function tail() { if (!openRun || paused || document.hidden) return; const url = '/api/runs/' + encodeURIComponent(openRun) + '/stream?max=2097152' + (offset !== null ? '&from=' + offset : ''); const response = await fetch(url).then(x => x.json()); appendTail(response.lines, offset === null); offset = response.next_offset; $('stream').scrollTop = $('stream').scrollHeight; }
  $('pause').onclick = () => { paused = !paused; put($('pause'), paused ? 'Resume tail' : 'Pause tail'); };
  $('start').onclick = event => { event.preventDefault(); openDetail(openRun, 0); };
  $('rawToggle').onclick = () => { rawMode = !rawMode; $('stream').classList.toggle('hidden', rawMode); $('rawStream').classList.toggle('hidden', !rawMode); put($('rawToggle'), rawMode ? 'Show rendered' : 'Show raw'); if (rawMode) renderRaw(); };
  $('moreRuns').onclick = () => runs(runsBefore);
  document.addEventListener('visibilitychange', () => { if (!document.hidden) { state(); runs(); tail(); } });
  state(); runs(); setInterval(state, 2000); setInterval(() => runs(), 5000); setInterval(tail, 1000);
})();
