# Replacing Celery with a Custom Scheduler

**Date:** 2025-10-29  
**Deciders:** Maziar (Backend Developer), Saman (Backend Lead), Kamran (CTO)  
**Tags:** recomm-system, custom scheduler and consumer, celery alternative

---

## Context

We used to use Celery and RabbitMQ to schedule and run the tasks of the recommendation system. Everything was fine until some long-term-running journeys/flows were created in production and started running for multiple users.

Suppose a journey with multiple steps, where one of its steps contains a task that should run a week from now. I refer you to this doc to understand the error:  
[Celery ETA and Countdown Tasks](https://docs.celeryq.dev/en/stable/userguide/calling.html)

Tasks with **eta** or **countdown** are immediately fetched by the worker, and until the scheduled time passes, they stay in the worker’s memory. When using those options to schedule lots of tasks for a distant future, those tasks may accumulate in the worker and make a significant impact on RAM usage. Moreover, tasks are not acknowledged until the worker starts executing them.

If using Redis as a broker, a task will get redelivered when the countdown exceeds `visibility_timeout`. But for RabbitMQ, it’s the opposite — when the countdown exceeds `visibility_timeout`, the task in RabbitMQ terminates and doesn’t wait for an acknowledgment.

You may say the solution is to make the `consumer_timeout` as long as possible, but that’s buggy! When using RabbitMQ as a broker, specifying a countdown over 15 minutes can cause the worker to terminate with a `PreconditionFailed` error:

amqp.exceptions.PreconditionFailed: (0, 0): (406) PRECONDITION_FAILED - consumer ack timed out on channel


---

## Decision

So we had to think of another way that didn’t include Celery. We needed a tool to **store and schedule tasks**, and a tool to **run them at the right time**.

---

## Rationale

We do not trust Celery anymore. We need something that gives us full control over how things are handled.

---

## Options Considered

### Option A: APScheduler

**Pros**
- It’s perfect and full of features.  
- No bugs or edge cases.

**Cons**
- It’s kind of over-engineering for our needs.  
- It’s like using a rocket launcher to kill a mosquito!  
- We’re also concerned about the number of packages being installed on the server.

---

### Option B: Celery Beat

**Pros**
- It’s a reliable tool from the Celery family.  
- Proven to handle long-term-running tasks well.

**Cons**
- It stores tasks in Postgres, and we don’t want to store recommendation tasks there.  
- We don’t need to keep track of this data.  
- Even if we remove the task data later, it would cause `VACUUM` overhead in Postgres.

---

### Option C: Writing Our Own Scheduler

**Pros**
- We can do everything exactly as needed.  
- We’ll only implement what we actually need — unlike APScheduler.

**Cons**
- It’s harder to implement.  
- It will take more time compared to other options.

---

## Consequences

Right now, our tasks in `action_list` are all **sync** because of Celery, which works synchronously. But if we switch to our own scheduler and consumer, we’ll have to **rewrite the old actions in an async way**.

We’ll also have to handle many edge cases that were previously taken care of by Celery’s “black box.” So we’re going to face some bugs we’re not aware of — and this is going to cost us time.

We’ll have to handle tasks in the background, manage task deletion (whether done or expired), and control how many tasks run at the same time so our resources don’t get fully occupied.

---

## Decision Details / Implementation Notes

We’ll store tasks and their args in a **Redis hash** and a **ZSET**.

- The hash will store task arguments.  
- The ZSET will act as a priority queue and store the due time of each task.  
- Both structures will map via the same `task_id`.

Then, we’ll create a **scheduler consumer** that constantly reads data from the ZSET and checks whether it’s time to run a task or not.

If it’s due, the consumer will run the task in the background using `asyncio.create_task`.

We’ll use something like **Semaphore** or **gather** to ensure only a certain number of tasks run at the same time.

- **Semaphore** acts like a window — it limits the number of concurrent tasks (e.g., 10). If one task fails, it’s retried, but other available slots continue processing new tasks.  
- **Gather**, on the other hand, waits for all tasks to finish. If one fails, the rest are blocked, and no new tasks start.

When using `create_task` without `gather`, we must make sure tasks aren’t destroyed by Python’s garbage collector. The event loop only holds weak references to them. In Python, anything without a strong reference can be destroyed.

We’ll handle this by **adding tasks to a set** and **removing them** when done using `add_done_callback`.  
Adding them to a set ensures strong references are maintained until completion.

---

## Change History

- **2025-10-31** — Written by Maziar
