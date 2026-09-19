import {test} from 'node:test';
import assert from 'node:assert/strict';
import {getEventListeners} from 'node:events';
import delay from 'delay';
import pDefer from 'p-defer';
import PQueue, {TimeoutError} from '../source/index.js';

/**
A countable `AbortSignal` stand-in.

Tracks how many `abort` listeners were registered and removed, and exposes
the number of listeners still attached. Built on a real `EventTarget` so
`once` and `removeEventListener` semantics match a genuine `AbortSignal`.
*/
class CountingAbortSignal extends EventTarget {
	#aborted = false;
	#reason: unknown;

	addListenerCount = 0;
	removeListenerCount = 0;

	onabort: AbortSignal['onabort'] = null;

	get aborted(): boolean {
		return this.#aborted;
	}

	get reason(): unknown {
		return this.#reason;
	}

	get listenerCount(): number {
		return getEventListeners(this, 'abort').length;
	}

	// eslint-disable-next-line @typescript-eslint/no-restricted-types -- Matches the EventTarget signature.
	override addEventListener(type: string, listener: EventListenerOrEventListenerObject | null, options?: AddEventListenerOptions | boolean): void {
		if (type === 'abort') {
			this.addListenerCount++;
		}

		super.addEventListener(type, listener, options);
	}

	// eslint-disable-next-line @typescript-eslint/no-restricted-types -- Matches the EventTarget signature.
	override removeEventListener(type: string, listener: EventListenerOrEventListenerObject | null, options?: EventListenerOptions | boolean): void {
		if (type === 'abort') {
			this.removeListenerCount++;
		}

		super.removeEventListener(type, listener, options);
	}

	throwIfAborted(): void {
		if (this.#aborted) {
			throw this.#reason;
		}
	}

	abort(reason: unknown = new Error('The operation was aborted')): void {
		if (this.#aborted) {
			return;
		}

		this.#aborted = true;
		this.#reason = reason;
		this.dispatchEvent(new Event('abort'));
	}
}

test('shared signal does not accumulate listeners across many short tasks', async () => {
	const queue = new PQueue({concurrency: 4});
	const signal = new CountingAbortSignal();

	const tasks = [];
	for (let index = 0; index < 50; index++) {
		tasks.push(queue.add(async () => index, {signal}));
	}

	await Promise.all(tasks);
	await queue.onIdle();

	assert.equal(signal.addListenerCount, 50);
	assert.equal(signal.removeListenerCount, 50);
	assert.equal(signal.listenerCount, 0);

	// Aborting after completion must not invoke any settled task's callback.
	signal.abort();
	assert.equal(signal.listenerCount, 0);
});

test('abort listener is removed when a task rejects', async () => {
	const queue = new PQueue();
	const signal = new CountingAbortSignal();

	const error = new Error('task failure');
	await assert.rejects(queue.add(async () => {
		throw error;
	}, {signal}), error);

	assert.equal(signal.addListenerCount, 1);
	assert.equal(signal.removeListenerCount, 1);
	assert.equal(signal.listenerCount, 0);
});

test('synchronously throwing task never registers an abort listener', async () => {
	const queue = new PQueue();
	const signal = new CountingAbortSignal();

	await assert.rejects(queue.add(() => {
		throw new Error('sync throw');
	}, {signal}));

	assert.equal(signal.addListenerCount, 0);
	assert.equal(signal.removeListenerCount, 0);
	assert.equal(signal.listenerCount, 0);
});

test('abort listener is removed when a task times out', async () => {
	const queue = new PQueue();
	const signal = new CountingAbortSignal();

	await assert.rejects(
		queue.add(async () => delay(100), {signal, timeout: 10}),
		TimeoutError,
	);

	assert.equal(signal.addListenerCount, 1);
	assert.equal(signal.removeListenerCount, 1);
	assert.equal(signal.listenerCount, 0);
});

test('pre-aborted task never registers an abort listener', async () => {
	const queue = new PQueue();
	const signal = new CountingAbortSignal();
	signal.abort();

	await assert.rejects(queue.add(async () => 'never runs', {signal}));

	assert.equal(signal.addListenerCount, 0);
	assert.equal(signal.removeListenerCount, 0);
	assert.equal(signal.listenerCount, 0);
});

test('task aborted while queued never registers an abort listener', async () => {
	const queue = new PQueue({concurrency: 1});
	const signal = new CountingAbortSignal();
	const {promise: blockerPromise, resolve: resolveBlocker} = pDefer<void>();

	const blocker = queue.add(() => blockerPromise);
	const queued = queue.add(async () => 'never runs', {signal});

	// Abort while the task is still waiting in the queue.
	signal.abort();
	resolveBlocker();

	await assert.rejects(queued);
	await blocker;

	assert.equal(signal.addListenerCount, 0);
	assert.equal(signal.removeListenerCount, 0);
	assert.equal(signal.listenerCount, 0);
});

test('cleanup of a settled task leaves other listeners on a shared signal intact', async () => {
	const queue = new PQueue({concurrency: 2});
	const signal = new CountingAbortSignal();
	const {promise: slowPromise, resolve: resolveSlow} = pDefer<string>();

	const fast = queue.add(async () => 'fast', {signal});
	const slow = queue.add(() => slowPromise, {signal});

	assert.equal(signal.listenerCount, 2);

	assert.equal(await fast, 'fast');
	// Only the settled task's own listener was removed.
	assert.equal(signal.listenerCount, 1);

	signal.abort();
	await assert.rejects(slow);
	assert.equal(signal.listenerCount, 0);
});

test('aborting one task does not affect the signal of another task', async () => {
	const queue = new PQueue({concurrency: 2});
	const signal1 = new CountingAbortSignal();
	const signal2 = new CountingAbortSignal();
	const {promise: promise1, resolve: resolve1} = pDefer<string>();
	const {promise: promise2, resolve: resolve2} = pDefer<string>();

	const task1 = queue.add(() => promise1, {signal: signal1});
	const task2 = queue.add(() => promise2, {signal: signal2});

	assert.equal(signal1.listenerCount, 1);
	assert.equal(signal2.listenerCount, 1);

	signal1.abort();
	await assert.rejects(task1);

	assert.equal(signal1.listenerCount, 0);
	assert.equal(signal1.removeListenerCount, 1);
	assert.equal(signal2.listenerCount, 1);
	assert.equal(signal2.removeListenerCount, 0);

	resolve2('ok');
	assert.equal(await task2, 'ok');
	assert.equal(signal2.listenerCount, 0);
});

test('task completing at the same moment as abort settles once and cleans up', async () => {
	const queue = new PQueue();
	const signal = new CountingAbortSignal();
	const {promise, resolve} = pDefer<string>();

	let settleCount = 0;
	let outcome: string;
	const task = queue.add(() => promise, {signal});

	// Resolve and abort within the same synchronous turn.
	resolve('done');
	signal.abort();

	try {
		await task;
		outcome = 'fulfilled';
	} catch {
		outcome = 'rejected';
	}

	settleCount++;

	assert.equal(outcome, 'fulfilled');
	assert.equal(settleCount, 1);
	assert.equal(signal.listenerCount, 0);
});

test('task aborted while running settles exactly once', async () => {
	const queue = new PQueue();
	const signal = new CountingAbortSignal();
	const {promise} = pDefer<string>();

	let settleCount = 0;
	let outcome: string;
	const task = queue.add(() => promise, {signal});

	signal.abort();

	try {
		await task;
		outcome = 'fulfilled';
	} catch {
		outcome = 'rejected';
	}

	settleCount++;

	assert.equal(outcome, 'rejected');
	assert.equal(settleCount, 1);
	assert.equal(signal.listenerCount, 0);

	// A later abort must not reach the settled task again.
	signal.abort();
	assert.equal(settleCount, 1);
});
