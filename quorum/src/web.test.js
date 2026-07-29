const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

const context = {globalThis: {}};
vm.runInNewContext(fs.readFileSync('quorum/src/web.js', 'utf8'), context);
const {stripShellWrapper, commandSummary, normalizeEvent} = context.globalThis.QuorumWeb;

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
assert.equal(normalizeEvent({type: 'future.event', payload: {}}).kind, 'unknown');
