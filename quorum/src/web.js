(() => {
  const MAX_RENDERED_TAIL_CHARS = 2 * 1024 * 1024;
  const MAX_RENDERED_TAIL_ROWS = 2000;
  const MAX_RENDERED_ROWS_PER_POLL = 2000;
  const MAX_NORMALIZED_RECORDS_PER_POLL = 2000;
  const MAX_PENDING_STREAM_BYTES = 8 * 1024 * 1024;
  const MAX_EXPANDED_OUTPUT_CHARS = 200 * 1024;
  const MAX_NORMALIZED_EVENTS_PER_RECORD = 100;
  const MAX_COCKPIT_ITEMS = 12;
  const MAX_TASK_LIST_ITEMS = 100;
  const MAX_DETAIL_ITEMS = 32;
  const MAX_DETAIL_HISTORY_ITEMS = 16;
  const MAX_DETAIL_TEXT_CHARS = 2 * 1024;
  const MAX_DETAIL_BODY_CHARS = 64 * 1024;
  const bounded = (items, max = MAX_COCKPIT_ITEMS) => Array.isArray(items) ? items.slice(0, max) : [];
  // `/api/tasks/:id` includes `dir` only when the server has meta-verified the matching
  // session log, so any stream navigation target is durable and task-specific.
  const navigableRuns = runs => bounded(runs).filter(run => typeof (run && run.dir) === 'string' && run.dir.length > 0);
  const textValue = value => value == null ? '' : String(value);
  const boundedText = (value, max = MAX_DETAIL_TEXT_CHARS) => {
    const text = textValue(value);
    return text.length > max ? text.slice(0, max - 1) + '…' : text;
  };
  const ellipsize = (text, max = 90) => text.length > max ? text.slice(0, max - 1) + '…' : text;

  function taskId(value) {
    const id = String(value == null ? '' : value);
    return /^[1-9]\d*$/.test(id) ? id : null;
  }

  function taskRoute(hash) {
    const match = String(hash || '').match(/^#task-([1-9]\d*)$/);
    return match ? match[1] : null;
  }

  function milestoneState(value) {
    return ['completed', 'current', 'future', 'terminal'].includes(value) ? value : 'future';
  }

  function journeyModel(progress) {
    const source = progress && typeof progress === 'object' ? progress : {};
    const milestones = bounded(source.milestones, MAX_DETAIL_HISTORY_ITEMS).map(milestone => ({
      stage: boundedText(milestone && milestone.stage), role: boundedText(milestone && milestone.role),
      activity: boundedText(milestone && milestone.activity), state: milestoneState(milestone && milestone.state),
    }));
    const history = bounded(source.history, MAX_DETAIL_HISTORY_ITEMS).map(milestone => ({
      stage: boundedText(milestone && milestone.stage), role: boundedText(milestone && milestone.role),
      activity: boundedText(milestone && milestone.activity), state: milestoneState(milestone && milestone.state),
    }));
    const stage = source.stage && typeof source.stage === 'object' ? {
      label: boundedText(source.stage.label), role: boundedText(source.stage.role), activity: boundedText(source.stage.activity),
    } : null;
    const next = source.next_action && typeof source.next_action === 'object' ? {
      label: boundedText(source.next_action.label), action: boundedText(source.next_action.action),
    } : null;
    return {milestones, history, stage, condition: boundedText(source.condition), next, attempts: source.attempts || {}, children: source.children || null};
  }

  function taskDetailModel(payload) {
    const source = payload && typeof payload === 'object' ? payload : {}, task = source.task && typeof source.task === 'object' ? source.task : {};
    return {
      task: {
        id: taskId(task.id), title: boundedText(task.title, 512), body: boundedText(task.body, MAX_DETAIL_BODY_CHARS),
        status: boundedText(task.status), labels: bounded(task.labels, MAX_DETAIL_ITEMS).map(label => boundedText(label)),
      },
      journey: journeyModel(source.progress),
      timeline: bounded(source.timeline, MAX_DETAIL_ITEMS), notes: bounded(source.notes, MAX_DETAIL_ITEMS),
      runs: bounded(source.runs, MAX_DETAIL_ITEMS), dependencies: bounded(task.dependencies, MAX_DETAIL_ITEMS),
      children: bounded(task.generated_children, MAX_DETAIL_ITEMS), generatedFrom: task.generated_from || null,
    };
  }

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

  function pollState(previous, result, now) {
    if (result === 'success') return {inFlight: false, lastSuccess: now, failed: false, stale: false};
    if (result === 'start') return {...previous, inFlight: true};
    return {inFlight: false, lastSuccess: previous.lastSuccess || null, failed: true, stale: true};
  }
  function dashboardModel(state) {
    const bands = state.queue_bands || {}, attention = state.needs_attention || {};
    const groups = [
      ['Blocked tasks', attention.blocked_tasks], ['Planning holds', attention.planning_holds],
      ['Merge waits', attention.merge_waits], ['Stalled agents', attention.stalled_agents],
      ['Agent errors', attention.error_agents], ['Alerts', attention.alerts], ['Recent errors', attention.recent_errors],
    ].filter(pair => Array.isArray(pair[1]) ? pair[1].length : Number(pair[1] && pair[1].count || 0));
    return {
      health: state.health || {}, attention: groups.slice(0, MAX_COCKPIT_ITEMS),
      working: bounded(state.working_now), queue: bounded(bands.ready && bands.ready.tasks),
      pipeline: [
        ['Planning', bands.planning && bands.planning.count || 0], ['Ready', bands.ready && bands.ready.count || 0],
        ['Working', bands.working && bands.working.count || 0], ['Reviewing', bands.reviewing && bands.reviewing.count || 0],
        ['Merging', bands.merging && bands.merging.count || 0],
        ['Terminal outcomes', Number(bands.terminal && bands.terminal.done || 0) + Number(bands.terminal && bands.terminal.failed || 0) + Number(bands.terminal && bands.terminal.cancelled || 0)],
      ],
    };
  }
  globalThis.QuorumWeb = {MAX_NORMALIZED_EVENTS_PER_RECORD, MAX_RENDERED_TAIL_ROWS, MAX_RENDERED_ROWS_PER_POLL, MAX_NORMALIZED_RECORDS_PER_POLL, MAX_PENDING_STREAM_BYTES, MAX_COCKPIT_ITEMS, MAX_TASK_LIST_ITEMS, MAX_DETAIL_ITEMS, MAX_DETAIL_HISTORY_ITEMS, MAX_DETAIL_TEXT_CHARS, stripShellWrapper, commandSummary, normalizeEvent, normalizeEvents, parseEventLine, normalizeTail, reassembleTail, detailNavigationState, liveAgentTitle, shouldTrim, bounded, boundedText, taskId, taskRoute, milestoneState, journeyModel, taskDetailModel, navigableRuns, textValue, pollState, dashboardModel};
  if (typeof document === 'undefined') return;

  let openRun = null, openRunTitle = null, offset = null, paused = false, rawMode = false, rawText = '', renderedChars = 0, runsBefore = null, tailState = {}, tailInFlight = false, tailEpoch = 0, runsEpoch = 0, currentRunsBefore = null, taskListBefore = null, taskListInFlight = false, taskDetailEpoch = 0, activeTaskId = null, stateInFlight = false, lastSuccess = null, stateFailed = false;
  const $ = id => document.getElementById(id);
  const put = (element, value) => { element.textContent = textValue(value); return element; };
  const node = value => put(document.createTextNode(''), value);
  function show(id) { document.querySelectorAll('main').forEach(element => element.classList.toggle('hidden', element.id !== id)); }
  document.querySelectorAll('[data-view]').forEach(button => button.addEventListener('click', () => {
    show(button.dataset.view);
    if (button.dataset.view === 'tasks') loadTasks();
  }));
  function appendRow(table, values, head = false) { const row = document.createElement('tr'); values.forEach(value => { const cell = document.createElement(head ? 'th' : 'td'); cell.append(value instanceof Node ? value : node(value)); row.append(cell); }); table.append(row); return row; }
  function age(seconds) { const value = Math.max(0, Number(seconds || 0)); return value < 60 ? value + 's' : value < 3600 ? Math.floor(value / 60) + 'm' : value < 86400 ? Math.floor(value / 3600) + 'h' : Math.floor(value / 86400) + 'd'; }
  function relativeTime(epoch, now) { return age(Math.max(0, now - epoch)) + ' ago'; }
  function timestamp(epoch) { return new Date(epoch * 1000).toLocaleString(); }
  function link(label, href, action) { const anchor = document.createElement('a'); anchor.href = href; put(anchor, label); if (action) anchor.addEventListener('click', event => { event.preventDefault(); action(); }); return anchor; }
  function taskLink(task) {
    const id = taskId(task && task.id), title = boundedText(task && task.title, 512);
    return id ? link('#' + id + (title ? ' ' + title : ''), '#task-' + encodeURIComponent(id), () => openTask(id)) : node('Task reference unavailable');
  }
  function prLink(pr) { return pr ? link('PR #' + pr, 'https://github.com/ag2trust/quorum/pull/' + encodeURIComponent(pr)) : node('No PR'); }
  function runLink(run, title) { return link(title || 'Run ' + run, '#run-' + encodeURIComponent(run), () => openDetail(run, null, title || 'Run ' + run)); }
  function agentLink(agent) { return agent.run_dir ? runLink(agent.run_dir, textValue(agent.name)) : link(textValue(agent.name), '#agents', () => show('agents')); }
  function empty(parent, message) { const paragraph = document.createElement('p'); paragraph.className = 'empty'; put(paragraph, message); parent.append(paragraph); }
  function item(parent, title, metadata, extraClass) {
    const entry = document.createElement('div'); entry.className = ('item ' + (extraClass || '')).trim();
    const heading = document.createElement('div'); heading.className = 'item-title'; heading.append(title instanceof Node ? title : node(title)); entry.append(heading);
    if (metadata && metadata.length) { const details = document.createElement('div'); details.className = 'meta'; metadata.forEach(value => details.append(value instanceof Node ? value : node(value))); entry.append(details); }
    parent.append(entry);
  }
  function trimStream() { const stream = $('stream'); while (shouldTrim(renderedChars, stream.childElementCount) && stream.firstChild) { renderedChars -= stream.firstChild.textContent.length; stream.firstChild.remove(); } }
  function exitBadge(code) { const badge = document.createElement('span'); badge.className = 'badge ' + (code === 0 ? 'ok' : code == null ? '' : 'bad'); put(badge, code == null ? 'in progress' : 'exit ' + code); return badge; }
  function capOutput(output) { return output.length > MAX_EXPANDED_OUTPUT_CHARS ? output.slice(0, MAX_EXPANDED_OUTPUT_CHARS) + '\n… output truncated …' : output; }
  function renderEvent(event) {
    if (event && event.type === 'item.started') return;
    const entry = event && event.kind ? event : normalizeEvent(event), row = document.createElement('div'); row.className = 'run-row run-' + entry.kind;
    if (entry.kind === 'message') put(row, entry.body);
    else if (entry.kind === 'command') { const details = document.createElement('details'), summary = document.createElement('summary'), pre = document.createElement('pre'); details.className = 'run-command'; put(summary, '$ ' + entry.title); summary.append(exitBadge(entry.exit_code)); put(pre, entry.body.command + '\n\n' + capOutput(entry.body.output)); details.append(summary, pre); row.append(details); }
    else { const title = document.createElement('span'); title.className = 'muted'; put(title, entry.title + (entry.body ? ' · ' + entry.body : '')); row.append(title); }
    renderedChars += row.textContent.length; $('stream').append(row); trimStream();
  }
  function renderRaw() { $('rawStream').textContent = rawText.length > MAX_RENDERED_TAIL_CHARS ? rawText.slice(-MAX_RENDERED_TAIL_CHARS) : rawText; }
  function appendTail(lines, partial, startsMidLine, omitted, replace) {
    if (replace) { $('stream').replaceChildren(); renderedChars = 0; rawText = ''; tailState = {}; }
    const reassembled = reassembleTail(tailState, lines, partial, startsMidLine, omitted); tailState = reassembled.state;
    const text = reassembled.lines.join('\n') + (reassembled.lines.length ? '\n' : ''); rawText = (rawText + text + (reassembled.truncated ? '[oversized stream record omitted]\n' : '')).slice(-MAX_RENDERED_TAIL_CHARS);
    const rendered = normalizeTail(reassembled.lines, MAX_RENDERED_ROWS_PER_POLL, MAX_NORMALIZED_RECORDS_PER_POLL, parseEventLine, reassembled.omitted);
    if (reassembled.truncated) rendered.unshift({kind: 'meta', title: 'oversized stream record omitted', body: '', exit_code: null});
    rendered.forEach(renderEvent); if (rawMode) renderRaw();
  }
  function renderHeader(health) {
    const verdict = health.verdict || 'unknown', badge = $('healthVerdict'); badge.dataset.verdict = verdict; put(badge, verdict.replace('-', ' '));
    const refresh = $('refreshStatus'), ageText = lastSuccess ? 'Last successful refresh ' + new Date(lastSuccess).toLocaleTimeString() : 'No successful refresh yet';
    refresh.classList.toggle('stale', stateFailed); put(refresh, stateFailed ? ageText + ' · data may be stale' : ageText);
  }
  function renderAttention(model) {
    const root = $('attention'); root.replaceChildren();
    if (!model.attention.length) return empty(root, 'No current attention signals.');
    model.attention.forEach(pair => {
      const label = pair[0], value = pair[1], count = Array.isArray(value) ? value.length : Number(value && value.count || 0), details = Array.isArray(value) ? value : bounded(value && value.tasks);
      const summary = document.createElement('div'); summary.className = 'attention'; summary.dataset.level = /error|stalled|blocked/i.test(label) ? 'error' : 'warning';
      const title = document.createElement('div'); title.className = 'item-title'; put(title, label + ': ' + count); summary.append(title);
      bounded(details, 3).forEach(record => { const line = document.createElement('div'); line.className = 'meta'; if (record && record.id != null) line.append(taskLink(record)); else if (record && record.name) line.append(agentLink(record)); else put(line, record && (record.body || record.detail || record.source || record.kind) || 'Attention signal'); summary.append(line); });
      root.append(summary);
    });
  }
  function renderWorking(working) {
    const root = $('workingNow'); root.replaceChildren();
    if (!working.length) return empty(root, 'No agent is actively working.');
    working.forEach(agent => {
      const title = agent.task_id == null ? agentLink(agent) : taskLink({id: agent.task_id, title: agent.task_title || 'Untitled task'});
      const activity = agent.last_activity_age_secs == null ? 'Last agent activity not reported' : 'Last agent activity ' + age(agent.last_activity_age_secs) + ' ago';
      const attention = agent.live_error_count > 0 ? 'Attention: ' + agent.live_error_count + ' live error(s)' : 'Attention: ' + (agent.agent_state || 'none reported');
      item(root, title, [(agent.role || 'agent') + ' · ' + (agent.phase || 'working'), agent.now || 'Activity detail not reported', agentLink(agent), activity, 'Time in current state not reported', attention, agent.pr ? prLink(agent.pr) : 'No PR']);
    });
  }
  function renderPipeline(pipeline) {
    const root = $('pipeline'); root.replaceChildren();
    pipeline.forEach(pair => { const cell = document.createElement('div'); cell.className = 'count'; const number = document.createElement('strong'); put(number, pair[1]); cell.append(number, node(pair[0])); root.append(cell); });
  }
  function renderQueue(queue) {
    const root = $('queue'); root.replaceChildren();
    if (!queue.length) return empty(root, 'No task is currently ready to claim.');
    queue.forEach(task => item(root, taskLink(task), ['Created ' + age(task.age_secs) + ' ago', 'Status: ' + (task.status || task.state || 'ready'), task.pr ? prLink(task.pr) : 'No PR']));
  }
  function renderDashboard(payload) {
    const model = dashboardModel(payload); renderHeader(model.health); renderAttention(model); renderWorking(model.working); renderPipeline(model.pipeline); renderQueue(model.queue);
    renderLiveAgents(payload.live_agents || []); renderAgents(payload.agents || [], payload.now || Math.floor(Date.now() / 1000));
  }
  function detailHeading(value) { const heading = document.createElement('h3'); put(heading, value); return heading; }
  function renderFact(root, label, value) {
    const fact = document.createElement('dl'); fact.className = 'fact'; const term = document.createElement('dt'), description = document.createElement('dd');
    put(term, label); description.append(value instanceof Node ? value : node(value || '—')); fact.append(term, description); root.append(fact);
  }
  function displayTimestamp(value) { return Number(value) > 0 ? timestamp(Number(value)) : '—'; }
  function listText(values) { return bounded(values, MAX_DETAIL_ITEMS).map(value => boundedText(value)).filter(Boolean).join(', ') || '—'; }
  function renderTaskFacts(task) {
    const root = $('taskFields'), readiness = task.readiness && typeof task.readiness === 'object' ? task.readiness : {}, rework = task.rework && typeof task.rework === 'object' ? task.rework : {};
    root.replaceChildren();
    renderFact(root, 'Status', boundedText(task.status)); renderFact(root, 'Priority', task.priority == null ? '—' : 'P' + boundedText(task.priority));
    renderFact(root, 'Labels', listText(task.labels)); renderFact(root, 'Assignee', boundedText(task.assignee));
    renderFact(root, 'Created by', boundedText(task.created_by)); renderFact(root, 'Author', boundedText(task.author)); renderFact(root, 'Reviewer', boundedText(task.reviewer));
    renderFact(root, 'Created', displayTimestamp(task.created_at)); renderFact(root, 'Updated', displayTimestamp(task.updated_at));
    renderFact(root, 'Branch', boundedText(task.branch)); renderFact(root, 'Pull request', task.pr ? prLink(task.pr) : '—');
    renderFact(root, 'Ready', readiness.ready == null ? '—' : readiness.ready ? 'Yes' : 'No');
    renderFact(root, 'Classification ready', readiness.classification_ready == null ? '—' : readiness.classification_ready ? 'Yes' : 'No');
    renderFact(root, 'Readiness reason', boundedText(readiness.reason));
    renderFact(root, 'Rework', rework.round == null ? '—' : 'Round ' + boundedText(rework.round) + ' of ' + boundedText(rework.cap));
    if (task.failure_reason) renderFact(root, 'Failure reason', boundedText(task.failure_reason));
    if (task.park_reason) renderFact(root, 'Park reason', boundedText(task.park_reason));
  }
  function renderJourney(journey) {
    const root = $('taskJourney'); root.replaceChildren();
    if (!journey.milestones.length) return empty(root, 'The server has not reported a journey projection.');
    journey.milestones.forEach(milestone => {
      const row = document.createElement('div'); row.className = 'milestone'; row.dataset.state = milestone.state;
      const state = document.createElement('div'); state.className = 'milestone-state'; put(state, milestone.state);
      const content = document.createElement('div'), title = document.createElement('div'); title.className = 'milestone-title'; put(title, milestone.stage || 'Lifecycle milestone'); content.append(title);
      if (milestone.role) { const role = document.createElement('div'); role.className = 'milestone-role'; put(role, milestone.role); content.append(role); }
      if (milestone.activity) { const activity = document.createElement('div'); put(activity, milestone.activity); content.append(activity); }
      row.append(state, content); root.append(row);
    });
  }
  function renderConditionAndNext(journey) {
    const condition = $('taskCondition'), next = $('taskNextAction'); condition.replaceChildren(); next.replaceChildren();
    if (journey.condition) { const box = document.createElement('div'); box.className = 'condition'; const label = document.createElement('strong'); put(label, 'Condition'); box.append(label, node(': ' + journey.condition)); condition.append(box); }
    if (journey.next && journey.next.action) { const box = document.createElement('div'); box.className = 'next-action'; const label = document.createElement('strong'); put(label, journey.next.label || 'Next'); box.append(label, node(': ' + journey.next.action)); next.append(box); }
  }
  function planningMilestone(milestone) { return milestone.stage === 'Planning' || milestone.stage === 'Plan review'; }
  function renderTaskHistory(journey) {
    const root = $('taskHistory'); root.replaceChildren(); const historical = journey.history.filter(planningMilestone);
    const currentPlanning = journey.stage && planningMilestone({stage: journey.stage.label}) ? journey.stage : null;
    if (!historical.length && !currentPlanning) return;
    root.append(detailHeading('Planning attempts and Arbiter outcomes'));
    const attempts = journey.attempts || {}, summary = document.createElement('p'); summary.className = 'muted';
    put(summary, 'Planning attempts / revisions: ' + boundedText(attempts.proposal_attempts || 0) + ' · Arbiter outcomes: ' + boundedText(attempts.arbiter_rounds || 0)); root.append(summary);
    const history = document.createElement('div'); history.className = 'history';
    historical.forEach((milestone, index) => {
      const details = document.createElement('details'), label = document.createElement('summary');
      put(label, milestone.stage + ' · Attempt ' + (index + 1) + (milestone.role ? ' · ' + milestone.role : '')); details.append(label);
      const outcome = document.createElement('div'); put(outcome, milestone.activity || 'Outcome recorded'); details.append(outcome); history.append(details);
    });
    if (currentPlanning) {
      const details = document.createElement('details'), label = document.createElement('summary'); details.open = true;
      put(label, currentPlanning.label + ' · Current attempt' + (currentPlanning.role ? ' · ' + currentPlanning.role : '')); details.append(label);
      const activity = document.createElement('div'); put(activity, currentPlanning.activity || 'In progress'); details.append(activity); history.append(details);
    }
    root.append(history);
  }
  function renderTaskReferences(model) {
    const root = $('taskRefs'); root.replaceChildren(); const raw = model.raw;
    const refs = document.createElement('div'); refs.className = 'meta'; if (raw.pr) refs.append(prLink(raw.pr)); if (raw.branch) refs.append(node('Branch: ' + boundedText(raw.branch))); root.append(refs);
    if (model.dependencies.length) { root.append(detailHeading('Dependencies')); const list = document.createElement('div'); list.className = 'meta'; model.dependencies.forEach(id => list.append(taskLink({id, title: 'dependency'}))); root.append(list); }
    if (model.generatedFrom && taskId(model.generatedFrom.task_id)) { root.append(detailHeading('Generated from')); const list = document.createElement('div'); list.className = 'meta'; list.append(taskLink({id: model.generatedFrom.task_id, title: model.generatedFrom.title})); root.append(list); }
    if (model.children.length) { root.append(detailHeading('Generated tasks')); const list = document.createElement('div'); list.className = 'meta'; model.children.forEach(child => list.append(taskLink(child))); root.append(list); }
  }
  function renderTextHistory(id, title, records, formatter) {
    const root = $(id); root.replaceChildren(); if (!records.length) return;
    root.append(detailHeading(title)); const list = document.createElement('div'); list.className = 'history';
    records.forEach(record => { const entry = document.createElement('div'); entry.className = 'item'; formatter(entry, record); list.append(entry); }); root.append(list);
  }
  function renderTimeline(records) {
    renderTextHistory('taskTimeline', 'Lifecycle events', records, (entry, event) => {
      const title = document.createElement('div'); title.className = 'item-title'; put(title, boundedText(event.kind) || 'Event');
      const meta = document.createElement('div'); meta.className = 'meta'; put(meta, displayTimestamp(event.ts) + (event.seq == null ? '' : ' · #' + boundedText(event.seq)));
      const body = document.createElement('div'); put(body, boundedText(event.body)); entry.append(title, meta, body);
    });
  }
  function renderNotes(records) {
    renderTextHistory('taskNotes', 'Notes', records, (entry, note) => {
      const title = document.createElement('div'); title.className = 'item-title'; put(title, boundedText(note.agent) || 'Note');
      const meta = document.createElement('div'); meta.className = 'meta'; put(meta, displayTimestamp(note.ts));
      const body = document.createElement('div'); put(body, boundedText(note.body)); entry.append(title, meta, body);
    });
  }
  function runSummary(run) {
    const end = run.ended_at ? 'Ended ' + displayTimestamp(run.ended_at) : 'In progress';
    return [boundedText(run.role) || 'agent', run.sub_role ? boundedText(run.sub_role) : '', boundedText(run.model), boundedText(run.effort), boundedText(run.provider), 'Started ' + displayTimestamp(run.spawned_at), end, boundedText(run.end_reason)].filter(Boolean);
  }
  function renderTaskRuns(records) {
    const root = $('taskRuns'); root.replaceChildren(); if (!records.length) return;
    root.append(detailHeading('Runs'));
    records.forEach(run => {
      const title = run.dir ? runLink(run.dir, 'Run #' + boundedText(run.id) + ' · ' + boundedText(run.agent)) : node('Run #' + boundedText(run.id) + ' · ' + boundedText(run.agent));
      item(root, title, runSummary(run));
    });
  }
  function renderTaskDetail(payload) {
    const model = taskDetailModel(payload), task = payload.task || {}; model.raw = task;
    put($('taskTitle'), '#' + (model.task.id || boundedText(task.id)) + ' ' + (model.task.title || 'Task detail'));
    $('taskMeta').replaceChildren(node('Status: ' + (model.task.status || '—')), node('Created: ' + displayTimestamp(task.created_at)), node('Journey: ' + boundedText(payload.progress && payload.progress.journey)));
    put($('taskBody'), model.task.body || 'No task description is available.'); renderTaskFacts(task); renderTaskReferences(model); renderJourney(model.journey); renderConditionAndNext(model.journey); renderTaskHistory(model.journey); renderTimeline(model.timeline); renderNotes(model.notes); renderTaskRuns(model.runs);
    return model;
  }
  async function openTask(requestedId, updateRoute = true) {
    const id = taskId(requestedId); if (!id) return;
    if (updateRoute && window.location.hash !== '#task-' + id) { window.location.hash = 'task-' + id; return; }
    if (activeTaskId === id && !$('task').classList.contains('hidden')) return;
    activeTaskId = id; const epoch = ++taskDetailEpoch;
    show('task'); put($('taskTitle'), 'Loading task #' + id + '…'); $('taskMeta').replaceChildren(); $('taskFields').replaceChildren(); $('taskBody').replaceChildren(); $('taskRefs').replaceChildren(); $('taskJourney').replaceChildren(); $('taskCondition').replaceChildren(); $('taskNextAction').replaceChildren(); $('taskHistory').replaceChildren(); $('taskTimeline').replaceChildren(); $('taskNotes').replaceChildren(); $('taskRuns').replaceChildren();
    try { const response = await fetch('/api/tasks/' + encodeURIComponent(id)); if (!response.ok) throw new Error('Task request failed'); const payload = await response.json(); if (epoch !== taskDetailEpoch) return; renderTaskDetail(payload); }
    catch (_) { if (epoch !== taskDetailEpoch) return; put($('taskTitle'), 'Task #' + id); put($('taskBody'), 'Task detail could not be loaded.'); }
  }
  function renderTaskList(tasks) {
    // Task-list pagination is page navigation, not an unbounded retained history.
    // The current page plus its header is always the whole task-list DOM.
    const table = $('taskList'); table.replaceChildren(); appendRow(table, ['Task', 'Status', 'Priority', 'Assignee', 'Updated'], true);
    bounded(tasks, MAX_TASK_LIST_ITEMS).forEach(task => appendRow(table, [taskLink(task), boundedText(task.status) || '—', task.priority == null ? '—' : 'P' + boundedText(task.priority), boundedText(task.assignee) || '—', displayTimestamp(task.updated_at)]));
  }
  async function loadTasks(cursor = null) {
    if (taskListInFlight) return; taskListInFlight = true;
    try {
      const suffix = cursor ? '?limit=' + MAX_TASK_LIST_ITEMS + '&cursor=' + encodeURIComponent(cursor) : '?limit=' + MAX_TASK_LIST_ITEMS;
      const response = await fetch('/api/tasks' + suffix); if (!response.ok) throw new Error('Task list request failed');
      const payload = await response.json(); renderTaskList(payload.tasks || []); taskListBefore = payload.next_cursor || null; $('moreTasks').classList.toggle('hidden', !taskListBefore);
    } catch (_) { if (!cursor) { $('taskList').replaceChildren(); appendRow($('taskList'), ['Tasks'], true); appendRow($('taskList'), ['Task list could not be loaded.']); } }
    finally { taskListInFlight = false; }
  }
  async function state() {
    if (document.hidden || stateInFlight) return false;
    ({inFlight: stateInFlight} = pollState({inFlight: stateInFlight, lastSuccess, failed: stateFailed, stale: stateFailed}, 'start', Date.now()));
    try { const response = await fetch('/api/state'); if (!response.ok) throw new Error('State request failed (' + response.status + ')'); const payload = await response.json(); const next = pollState({inFlight: stateInFlight, lastSuccess, failed: stateFailed, stale: stateFailed}, 'success', Date.now()); stateInFlight = next.inFlight; lastSuccess = next.lastSuccess; stateFailed = next.failed; renderDashboard(payload); return true; }
    catch (_) { const next = pollState({inFlight: stateInFlight, lastSuccess, failed: stateFailed, stale: stateFailed}, 'failure', Date.now()); stateInFlight = next.inFlight; lastSuccess = next.lastSuccess; stateFailed = next.failed; renderHeader({verdict: 'attention'}); return false; }
  }
  function renderLiveAgents(agents) {
    const table = $('liveAgentTable'); table.replaceChildren(); appendRow(table, ['Agent', 'Role / phase', 'Task', 'Activity'], true);
    bounded(agents, 100).forEach(agent => appendRow(table, [agentLink(agent), (agent.role || '—') + ' / ' + (agent.phase || '—'), agent.task_id == null ? '—' : taskLink({id: agent.task_id, title: agent.task_title || ''}), agent.last_activity_age_secs == null ? '—' : age(agent.last_activity_age_secs) + ' ago']));
  }
  function renderAgents(agents, now) {
    const online = agents.filter(agent => agent.online), offline = agents.filter(agent => !agent.online);
    const render = (table, rows) => { table.replaceChildren(); appendRow(table, ['Agent', 'Task', 'Last seen'], true); bounded(rows, 100).forEach(agent => { const seen = document.createElement('span'); put(seen, relativeTime(agent.last_seen, now)); seen.title = timestamp(agent.last_seen); appendRow(table, [agentLink(agent), agent.task_held ? taskLink(agent.task_held) : '—', seen]); }); };
    render($('agentTable'), online); render($('offlineAgentTable'), offline); $('offlineAgents').classList.toggle('hidden', !offline.length); put($('offlineAgents').querySelector('summary'), offline.length + ' offline agents (secondary)');
  }
  function duration(meta) { return meta.start_time && meta.end_time ? age(Math.max(0, meta.end_time - meta.start_time)) : '—'; }
  function renderRuns(items, replace) {
    const table = $('runs'); if (replace) { table.replaceChildren(); appendRow(table, ['Agent', 'Role', 'Task', 'Duration', 'Final phase'], true); }
    bounded(items, 100).forEach(run => { const meta = run.meta || {}; appendRow(table, [runLink(run.dir, meta.agent || run.dir), meta.role || '—', meta.task_id == null ? '—' : taskLink({id: meta.task_id, title: ''}), duration(meta), meta.final_phase || '—']); });
  }
  async function runs(before = currentRunsBefore, explicit = false) {
    if (document.hidden || (!explicit && currentRunsBefore !== null)) return;
    if (explicit) currentRunsBefore = before;
    const epoch = ++runsEpoch, suffix = before ? '?before=' + encodeURIComponent(before) : '';
    try { const response = await fetch('/api/runs' + suffix).then(result => result.json()); if (epoch !== runsEpoch) return; renderRuns(response.runs || [], true); currentRunsBefore = before; runsBefore = response.next_before; $('moreRuns').classList.toggle('hidden', !runsBefore); $('newestRuns').classList.toggle('hidden', !currentRunsBefore); } catch (_) {}
  }
  async function openDetail(dir, from, title = dir) {
    tailEpoch++; const next = detailNavigationState(dir, from); ({openRun, offset, paused, rawMode, rawText, renderedChars, tailState} = next); openRunTitle = title;
    $('stream').replaceChildren(); put($('pause'), 'Pause tail'); put($('rawToggle'), 'Show raw'); $('rawStream').classList.add('hidden'); $('stream').classList.remove('hidden'); show('run'); put($('runTitle'), title); await tail();
  }
  async function tail() {
    if (!openRun || paused || document.hidden || tailInFlight) return;
    tailInFlight = true; const epoch = tailEpoch, run = openRun, from = offset;
    try { const url = '/api/runs/' + encodeURIComponent(run) + '/stream?max=2097152' + (from !== null ? '&from=' + from : ''); const response = await fetch(url).then(result => result.json()); if (epoch !== tailEpoch || run !== openRun) return; appendTail(response.lines, response.partial, response.starts_mid_line, response.omitted || 0, from === null); offset = response.next_offset; $('stream').scrollTop = $('stream').scrollHeight; } finally { tailInFlight = false; if (epoch !== tailEpoch) tail(); }
  }
  $('pause').onclick = () => { paused = !paused; put($('pause'), paused ? 'Resume tail' : 'Pause tail'); };
  $('backToCockpit').onclick = event => { event.preventDefault(); if (taskRoute(window.location.hash)) window.location.hash = ''; else show('board'); };
  $('start').onclick = event => { event.preventDefault(); openDetail(openRun, 0, openRunTitle); };
  $('rawToggle').onclick = () => { rawMode = !rawMode; $('stream').classList.toggle('hidden', rawMode); $('rawStream').classList.toggle('hidden', !rawMode); put($('rawToggle'), rawMode ? 'Show rendered' : 'Show raw'); if (rawMode) renderRaw(); };
  $('moreRuns').onclick = () => runs(runsBefore, true);
  $('newestRuns').onclick = () => runs(null, true);
  $('moreTasks').onclick = () => loadTasks(taskListBefore);
  function routeFromHash() {
    const id = taskRoute(window.location.hash);
    if (id) return openTask(id, false);
    if (activeTaskId !== null) taskDetailEpoch++;
    activeTaskId = null; show('board');
  }
  window.addEventListener('hashchange', routeFromHash);
  document.addEventListener('visibilitychange', () => { if (!document.hidden) { state(); runs(); tail(); } });
  routeFromHash(); state(); runs(); setInterval(state, 2000); setInterval(() => runs(), 5000); setInterval(tail, 1000);
})();
