const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

const context = {globalThis: {}};
vm.runInNewContext(fs.readFileSync('quorum/src/web.js', 'utf8'), context);
const {stripShellWrapper, commandSummary, normalizeEvent, normalizeEvents, parseEventLine} = context.globalThis.QuorumWeb;

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
