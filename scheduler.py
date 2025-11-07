import json, logging
from .redis import redis_client  # Implement this on your own!

RECOMM_JOB_STORE = "log:scheduler:jobs"
RECOMM_JOBS_RUN_TIME_KEY = "log:scheduler:run_times"
logger = logging.getLogger(__name__)


async def schedule_task(task_id, run_at, **kwargs):
    async with redis_client.pipeline() as pipe:
        await pipe.hset(
            RECOMM_JOB_STORE,
            task_id,
            json.dumps(
                {
                    "id": task_id,
                    "kwargs": kwargs,
                    "next_run_time": run_at,  # run_at is timestamp
                }
            ),
        )
        await pipe.zadd(RECOMM_JOBS_RUN_TIME_KEY, {task_id: run_at})
        try:
            await pipe.execute()
        except Exception as pipe_err:
            logger.error(f"Error occured when adding the task to redis: {pipe_err}")
