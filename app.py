from fastapi import FastAPI, BackgroundTasks
from fastapi.responses import JSONResponse
import utils

app = FastAPI()

@app.get("/")
def download_video_api(url: str, background_tasks: BackgroundTasks):
    video_id = utils.generate_unique_id()
    task = utils.get_task(video_id)
    if task.get('status') == 'done':
        return JSONResponse(content=task)
    background_tasks.add_task(utils.task_handler, url=url, video_id=video_id)
    task = utils.update_task(video_id, 'in progress')
    return JSONResponse(content=task)

@app.get("/task/{id}")
def get_task_api(id: int):
    return utils.get_task(id)