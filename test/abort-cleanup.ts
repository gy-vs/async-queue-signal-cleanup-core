import {test} from 'node:test';
import assert from 'node:assert/strict';
import pDefer from 'p-defer';
import PQueue, {TimeoutError} from '../source/index.js';

type AbortListener = () => void;

/**
A countable `AbortSignal` stand-in.

Implements only the `AbortSignal` surface p-queue relies on and counts
listener registrations, removals and dispatches, so tests can assert that
every task detaches exactly its own abort listener once it settles.
*/
class CountingAbortSignal {
	readonly #listeners = new Map<AbortListener, {once: boolean}>();
	#aborted = false;
	#reason: unknown;

	/** Total 'abort' listeners ever registered. */
	addCount = 0;

	/** Total listeners explicitly removed via `removeEventListener`. */
	removeCount = 0;

	/** Listeners actually invoked by `abort()`. */
	dispatchCount = 0;

	get aborted(): boolean {
		return this.#aborted;
	}

	get reason(): unknown {
		return this.#reason;
	}

	/** Currently registered 'abort' listeners. */
	get listenerCount(): number {
		return this.#listeners.size;
	}

	addEventListener(type: string, listener: AbortListener, options?: {once?: boolean}): void {
		if (type !== 'abort') {
			return;
		}

		this.addCount++;
		this.#listeners.set(listener, {once: options?.once ?? false});
	}

	removeEventListener(type: string, listener: AbortListener): void {
		if (type !== 'abort' || !this.#listeners.has(listener)) {
			return;
		}

		this.removeCount++;
		this.#listeners.delete(listener);
	}

	throwIfAborted(): void {
		if (this.#aborted) {
			throw this.#reason;
		}
	}

	abort(reason: unknown = new Error('This operation was aborted')): void {
		if (this.#aborted) {
			return;
		}

		this.#aborted = true;
		this.#reason = reason;

		// Snapshot before dispatch: `once` listeners are removed while iterating.
		// eslint-disable-next-line unicorn/no-useless-spread
		for (const [listener, {once}] of [...this.#listeners]) {
			if (once) {
				this.#listeners.delete(listener);
			}

			this.dispatchCount++;
			listener();
		}
	}
}

function createCountingSignal(): {signal: AbortSignal; mock: CountingAbortSignal} {
	const mock = new CountingAbortSignal();
	return {signal: mock as unknown as AbortSignal, mock};
}

test('shared signal: listeners are removed as tasks settle', async () => {
	const queue = new PQueue({concurrency: 4});
	const {signal, mock} = createCountingSignal();

	const tasks = [];
	for (let index = 0; index < 50; index++) {
		tasks.push(queue.add(async () => index, {signal}));
	}

	const results = await Promise.all(tasks);
	assert.equal(results[49], 49);
	await queue.onIdle();

	assert.equal(mock.addCount, 50);
	assert.equal(mock.removeCount, 50);
	assert.equal(mock.listenerCount, 0);

	// Aborting after everything settled must not reach any finished task.
	mock.abort(new Error('too late'));
	assert.equal(mock.dispatchCount, 0);
});

test('listener is removed before the add() promise reactions run', async () => {
	const queue = new PQueue();
	const {signal, mock} = createCountingSignal();

	let countAtReaction = -1;
	const task = queue.add(() => 'done', {signal});
	// eslint-disable-next-line promise/prefer-await-to-then
	task.then(() => {
		countAtReaction = mock.listenerCount;
	});

	await task;
	assert.equal(countAtReaction, 0);
});

test('failure path: rejected task removes its abort listener', async () => {
	const queue = new PQueue();
	const {signal, mock} = createCountingSignal();
	const boom = new Error('boom');

	await assert.rejects(
		queue.add(async () => {
			throw boom;
		}, {signal}),
		thrown => thrown === boom,
	);

	assert.equal(mock.addCount, 1);
	assert.equal(mock.removeCount, 1);
	assert.equal(mock.listenerCount, 0);
});

test('timeout path: timed-out task removes its abort listener', async () => {
	const queue = new PQueue();
	const {signal, mock} = createCountingSignal();

	await assert.rejects(
		// eslint-disable-next-line @typescript-eslint/no-empty-function
		queue.add(async () => new Promise(() => {}), {signal, timeout: 20}),
		TimeoutError,
	);

	assert.equal(mock.addCount, 1);
	assert.equal(mock.removeCount, 1);
	assert.equal(mock.listenerCount, 0);
});

test('sync throw path: synchronously throwing task registers nothing and leaves nothing', async () => {
	const queue = new PQueue();
	const {signal, mock} = createCountingSignal();
	const boom = new Error('sync boom');

	await assert.rejects(
		queue.add(() => {
			throw boom;
		}, {signal}),
		thrown => thrown === boom,
	);

	assert.equal(mock.addCount, 0);
	assert.equal(mock.listenerCount, 0);
	await queue.onIdle();
});

test('pre-aborted signal: rejects without ever registering a listener', async () => {
	const queue = new PQueue();
	const {signal, mock} = createCountingSignal();
	const reason = new Error('aborted before add');
	mock.abort(reason);

	await assert.rejects(
		queue.add(() => 'never runs', {signal}),
		thrown => thrown === reason,
	);

	assert.equal(mock.addCount, 0);
	assert.equal(mock.listenerCount, 0);
});

test('abort while queued: nothing is registered, rejection is delivered on dequeue', async () => {
	const queue = new PQueue({concurrency: 1});
	const {signal, mock} = createCountingSignal();
	const deferred = pDefer<string>();

	const running = queue.add(() => deferred.promise);
	const queued = queue.add(() => 'never runs', {signal});

	const reason = new Error('cancelled while queued');
	mock.abort(reason);

	// The queued task never got a chance to register anything.
	assert.equal(mock.addCount, 0);
	assert.equal(mock.listenerCount, 0);

	deferred.resolve('done');
	assert.equal(await running, 'done');
	await assert.rejects(queued, thrown => thrown === reason);

	assert.equal(mock.addCount, 0);
	assert.equal(mock.listenerCount, 0);
});

test('abort while running: rejects and removes the listener', async () => {
	const queue = new PQueue();
	const {signal, mock} = createCountingSignal();
	const reason = new Error('stop');

	// eslint-disable-next-line @typescript-eslint/no-empty-function
	const task = queue.add(async () => new Promise(() => {}), {signal});
	assert.equal(mock.listenerCount, 1);

	mock.abort(reason);
	await assert.rejects(task, thrown => thrown === reason);

	assert.equal(mock.listenerCount, 0);
	await queue.onIdle();
});

test('cleanup removes only the settling task’s own listener', async () => {
	const queue = new PQueue({concurrency: 2});
	const {signal, mock} = createCountingSignal();
	const deferred = pDefer<string>();

	const first = queue.add(() => 'first', {signal});
	const second = queue.add(() => deferred.promise, {signal});

	assert.equal(mock.listenerCount, 2);
	assert.equal(await first, 'first');

	// The settled task detached; the running task is still protected.
	assert.equal(mock.listenerCount, 1);

	const reason = new Error('stop');
	mock.abort(reason);
	await assert.rejects(second, thrown => thrown === reason);
	assert.equal(mock.listenerCount, 0);
});

test('task completes and aborts at the same moment (abort inside the task)', async () => {
	const queue = new PQueue();
	const {signal, mock} = createCountingSignal();

	// The task aborts the signal synchronously while running, then completes.
	const result = await queue.add(() => {
		mock.abort(new Error('abort during task'));
		return 'done';
	}, {signal});

	// The result settled exactly once, with the task's value.
	assert.equal(result, 'done');
	// The listener registered after the abort was still cleaned up.
	assert.equal(mock.addCount, 1);
	assert.equal(mock.removeCount, 1);
	assert.equal(mock.listenerCount, 0);
});

test('task completes and aborts at the same moment (abort in a completed handler)', async () => {
	const queue = new PQueue();
	const {signal, mock} = createCountingSignal();

	queue.on('completed', () => {
		mock.abort(new Error('abort on completed'));
	});

	const result = await queue.add(() => 'done', {signal});

	// Settled exactly once with the task result; the concurrent abort is ignored.
	assert.equal(result, 'done');
	// The listener was still attached when abort fired, then got cleaned up.
	assert.equal(mock.dispatchCount, 1);
	assert.equal(mock.listenerCount, 0);
});

test('success, failure, timeout and abort share one signal without leaking', async () => {
	const queue = new PQueue({concurrency: 4});
	const {signal, mock} = createCountingSignal();

	const success = queue.add(async () => 'ok', {signal});
	const failure = queue.add(async () => {
		throw new Error('boom');
	}, {signal});
	// eslint-disable-next-line @typescript-eslint/no-empty-function
	const timedOut = queue.add(async () => new Promise(() => {}), {signal, timeout: 20});
	// eslint-disable-next-line @typescript-eslint/no-empty-function
	const aborted = queue.add(async () => new Promise(() => {}), {signal});

	assert.equal(mock.listenerCount, 4);

	assert.equal(await success, 'ok');
	await assert.rejects(failure, {message: 'boom'});
	await assert.rejects(timedOut, TimeoutError);

	// Three tasks settled and detached; only the still-running task remains.
	assert.equal(mock.listenerCount, 1);

	mock.abort(new Error('stop'));
	await assert.rejects(aborted, {message: 'stop'});

	// The abort reached only the task that was still running.
	assert.equal(mock.dispatchCount, 1);
	assert.equal(mock.listenerCount, 0);
	await queue.onIdle();
});
