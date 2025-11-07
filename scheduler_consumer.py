import asyncio, time, json, asyncio, logging
from .redis import redis_client
from .task_processor import process_user_action
from .scheduler_consumer import RECOMM_JOB_STORE, RECOMM_JOBS_RUN_TIME_KEY

logger = logging.getLogger(__name__)
tasks_to_run = asyncio.Semaphore(10)
active_tasks: set[asyncio.Task] = set()


def _task_done_callback(task: asyncio.Task, task_id: str):
    active_tasks.discard(task)

    try:
        exc = task.exception()  # If everything is alright None would be returned
    except asyncio.CancelledError:
        logger.warning(f"[Cancelled] Task {task_id} was cancelled")
        return
    except Exception as e:
        logger.exception(f"[CallbackError] Getting exception for {task_id}: {e}")
        return

    if exc is not None:
        logger.exception(f"[TaskException] Task {task_id} raised: {exc}")
    else:
        logger.info(f"[TaskCallback] Task {task_id} finished successfully")


async def execute_task(task_id: str, job_data: dict):
    try:
        await process_user_action(**job_data["kwargs"])
    except Exception as e:
        logger.exception(f"[Error] Task {task_id}: {e}")
    finally:
        try:
            tasks_to_run.release()
        except Exception as release_semaphore_err:
            logger.error(
                f"Error occured when releasing semaphore: {release_semaphore_err}"
            )


async def run_due_tasks():
    now = time.time()

    try:
        task_ids = await redis_client.zrangebyscore(
            RECOMM_JOBS_RUN_TIME_KEY, 0, now, start=0, num=200
        )
    except Exception as get_task_err:
        logger.error(f"[RedisError] Failed to fetch task IDs: {get_task_err}")
        task_ids = []

    if not task_ids:
        await asyncio.sleep(1)

    for task_id in task_ids:
        try:
            raw = await redis_client.hget(RECOMM_JOB_STORE, task_id)
            if not raw:
                logger.error(f"[Skip] No data for {task_id}")
                await redis_client.zrem(RECOMM_JOBS_RUN_TIME_KEY, task_id)
                continue
            job_data = json.loads(raw)
            try:
                await tasks_to_run.acquire()
                task = asyncio.create_task(execute_task(task_id, job_data))
                active_tasks.add(task)
                task.add_done_callback(
                    lambda t, tid=task_id: _task_done_callback(t, tid)
                )

                try:
                    async with redis_client.pipeline() as pipe:
                        await pipe.hdel(RECOMM_JOB_STORE, task_id)
                        await pipe.zrem(RECOMM_JOBS_RUN_TIME_KEY, task_id)
                        await pipe.execute()
                    logger.info(f"Task {task_id} cleaned up")
                except Exception as pipe_err:
                    logger.error(
                        f"Error occured when removing the tasks from redis: {pipe_err}"
                    )

            except Exception as task_err:
                logger.error(f"[TaskError] Task {task_id} failed: {task_err}")

        except Exception as unexpected_err:
            logger.error(f"Unexpected error happened: {unexpected_err}")
            try:
                async with redis_client.pipeline() as pipe:
                    await pipe.hdel(RECOMM_JOB_STORE, task_id)
                    await pipe.zrem(RECOMM_JOBS_RUN_TIME_KEY, task_id)
                    try:
                        await pipe.execute()
                    except Exception as pipe_err:
                        logger.error(
                            f"Error occured when removing the tasks from redis: {pipe_err}"
                        )
            except:
                pass


async def main():
    logger.info("Scheduler started. Waiting for due tasks...")
    while True:
        try:
            await run_due_tasks()
        except Exception as e:
            logger.error(f"Exception: {type(e)}: {e}")


if __name__ == "__main__":
    asyncio.run(main())
