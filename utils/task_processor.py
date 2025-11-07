async def process_user_action(task_input: dict):
    pass
    # This is a kind of method which is going to run the tasks stored in redis.
    # This should be able to schedule a task multiple times and check if there are more steps for the task
    # So instead of storing the task method obj in redis we just store the args of the task and just keep
    # list of the tasks and run them using this process_user_action method.
